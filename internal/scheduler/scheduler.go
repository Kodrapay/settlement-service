package scheduler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kodra-pay/settlement-service/internal/dto"
	"github.com/kodra-pay/settlement-service/internal/services"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type SettlementScheduler struct {
	db                 *sql.DB
	redis              *redis.Client
	settlementSvc      *services.SettlementService
	ticker             *time.Ticker
	stopChan           chan bool
	checkIntervalMin   int
	delayMinutes       int
	merchantServiceURL string
}

func NewSettlementScheduler(db *sql.DB, settlementSvc *services.SettlementService, merchantServiceURL string) *SettlementScheduler {
	// Initialize Redis client
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis:6379"
	}

	// Strip redis:// scheme if present
	if len(redisURL) > 8 && redisURL[:8] == "redis://" {
		redisURL = redisURL[8:]
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Test connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Failed to connect to Redis at %s: %v", redisURL, err)
	} else {
		log.Printf("Settlement scheduler connected to Redis at %s", redisURL)
	}

	return &SettlementScheduler{
		db:                 db,
		redis:              redisClient,
		settlementSvc:      settlementSvc,
		checkIntervalMin:   1, // Check every minute for the demo
		delayMinutes:       5, // T+1 simulated as 5 minutes for demo
		stopChan:           make(chan bool),
		merchantServiceURL: merchantServiceURL,
	}
}

// Start begins the settlement scheduler
func (s *SettlementScheduler) Start() {
	log.Printf("Starting settlement scheduler (checking every %d minutes)", s.checkIntervalMin)
	s.ticker = time.NewTicker(time.Duration(s.checkIntervalMin) * time.Minute)

	// Run immediately on start
	go s.processSettlements()

	// Then run on schedule
	go func() {
		for {
			select {
			case <-s.ticker.C:
				s.processSettlements()
			case <-s.stopChan:
				s.ticker.Stop()
				return
			}
		}
	}()
}

// Stop stops the settlement scheduler
func (s *SettlementScheduler) Stop() {
	log.Println("Stopping settlement scheduler")
	s.stopChan <- true
}

// processSettlements checks for and processes pending settlements
func (s *SettlementScheduler) processSettlements() {
	ctx := context.Background()
	log.Println("Checking for settlements due...")

	// Current time helpers for filters
	now := time.Now()
	currentTime := now.Format("15:04:05")
	dayOfWeek := int(now.Weekday())
	if dayOfWeek == 0 {
		dayOfWeek = 7 // Sunday = 7
	}

	// OPTIMIZATION: collect merchants to check from Redis + DB safety net (pending balance > 0)
	var merchantsToCheck []int64
	merchantSet := make(map[int64]struct{})

	if s.redis != nil {
		pendingMerchants, err := s.redis.SMembers(ctx, "settlements:merchants:pending").Result()
		if err != nil {
			log.Printf("Warning: Failed to get pending merchants from Redis: %v", err)
		} else if len(pendingMerchants) > 0 {
			for _, m := range pendingMerchants {
				if mid, err := strconv.ParseInt(m, 10, 64); err == nil {
					merchantSet[mid] = struct{}{}
				}
			}
			log.Printf("Found %d merchants with pending transactions in Redis", len(pendingMerchants))
		}
	}

	// Safety net: include any merchant with pending_balance > 0 and auto_settle enabled
	dbRows, dbErr := s.db.QueryContext(ctx, `
		SELECT DISTINCT sc.merchant_id
		FROM settlement_configs sc
		JOIN merchants m ON sc.merchant_id = m.id
		JOIN merchant_balances mb ON mb.merchant_id = sc.merchant_id AND mb.currency = sc.currency
		WHERE sc.auto_settle = true
		  AND m.status = 'active'
		  AND m.kyc_status = 'approved'
		  AND sc.settlement_time <= $1
		  AND (
		      sc.schedule_type = 'daily'
		      OR (sc.schedule_type = 'weekly' AND $2 = ANY(sc.settlement_days))
		  )
		  AND mb.pending_balance > 0
	`, currentTime, dayOfWeek)
	if dbErr != nil {
		log.Printf("Warning: failed to query DB for pending merchants: %v", dbErr)
	} else {
		defer dbRows.Close()
		for dbRows.Next() {
			var mid int64
			if scanErr := dbRows.Scan(&mid); scanErr == nil {
				merchantSet[mid] = struct{}{}
			}
		}
	}

	for mid := range merchantSet {
		merchantsToCheck = append(merchantsToCheck, mid)
	}

	if len(merchantsToCheck) > 0 {
		log.Printf("Merchants to check: %v", merchantsToCheck)
	} else {
		log.Println("No merchants found in Redis or DB pending list; falling back to full scan.")
	}

	// Build query based on whether we have specific merchants to check
	var query string
	var rows *sql.Rows
	var err error

	if len(merchantsToCheck) > 0 {
		// OPTIMIZED: Only check merchants that have pending transactions
		query = `
			SELECT sc.merchant_id, sc.schedule_type, sc.settlement_time, sc.settlement_days,
			       sc.minimum_amount, sc.settlement_delay_days, sc.currency,
			       m.name, m.email, m.business_name
			FROM settlement_configs sc
			JOIN merchants m ON sc.merchant_id = m.id
			WHERE sc.auto_settle = true
			  AND m.status = 'active'
			  AND m.kyc_status = 'approved'
			  AND sc.settlement_time <= $1
			  AND sc.merchant_id = ANY($2)
			  AND (
			      sc.schedule_type = 'daily'
			      OR (sc.schedule_type = 'weekly' AND $3 = ANY(sc.settlement_days))
			  )
		`
		rows, err = s.db.QueryContext(ctx, query, currentTime, pq.Array(merchantsToCheck), dayOfWeek)
	} else {
		// Fallback: Check all merchants (old behavior)
		query = `
			SELECT sc.merchant_id, sc.schedule_type, sc.settlement_time, sc.settlement_days,
			       sc.minimum_amount, sc.settlement_delay_days, sc.currency,
			       m.name, m.email, m.business_name
			FROM settlement_configs sc
			JOIN merchants m ON sc.merchant_id = m.id
			WHERE sc.auto_settle = true
			  AND m.status = 'active'
			  AND m.kyc_status = 'approved'
			  AND sc.settlement_time <= $1
			  AND (
			      sc.schedule_type = 'daily'
			      OR (sc.schedule_type = 'weekly' AND $2 = ANY(sc.settlement_days))
			  )
		`
		rows, err = s.db.QueryContext(ctx, query, currentTime, dayOfWeek)
	}

	if err != nil {
		log.Printf("Error querying settlement configs: %v", err)
		return
	}
	defer rows.Close()

	processedCount := 0
	for rows.Next() {
		var (
			merchantID          int
			scheduleType        string
			settlementTime      string
			settlementDays      pq.Int64Array
			minimumAmount       int64
			settlementDelayDays int
			currency            string
			merchantName        string
			merchantEmail       string
			businessName        string
		)

		err := rows.Scan(
			&merchantID, &scheduleType, &settlementTime, &settlementDays,
			&minimumAmount, &settlementDelayDays, &currency,
			&merchantName, &merchantEmail, &businessName,
		)
		if err != nil {
			log.Printf("Error scanning settlement config: %v", err)
			continue
		}

		// Check if we've already processed a settlement today for this merchant
		if s.hasSettledToday(ctx, strconv.Itoa(merchantID)) {
			log.Printf("Merchant %s (%d) already settled today, skipping", businessName, merchantID)
			continue
		}

		// Get merchant's full balance (pending and available)
		merchantBalance, err := s.getMerchantBalance(ctx, merchantID)
		if err != nil {
			log.Printf("Error getting full balance for merchant %d: %v", merchantID, err)
			continue
		}

		// Check if pending balance is positive and meets minimum
		if merchantBalance.PendingBalance <= 0 {
			log.Printf("Merchant %s (%d) has no pending balance, skipping", businessName, merchantID)
			// Clear Redis pending data since there's nothing to settle
			if s.redis != nil {
				s.clearMerchantPending(ctx, strconv.Itoa(merchantID))
			}
			continue
		}

		if merchantBalance.PendingBalance < minimumAmount {
			log.Printf("Merchant %s (%d) pending balance %d kobo below minimum %d kobo, skipping",
				businessName, merchantID, merchantBalance.PendingBalance, minimumAmount)
			continue
		}

		// Create settlement using the pending balance (wallet to wallet)
		settlementID, err := s.createSettlement(ctx, merchantID, merchantBalance.PendingBalance, currency)
		if err != nil {
			log.Printf("Error creating settlement for merchant %d: %v", merchantID, err)
			continue
		}

		log.Printf("Created settlement %s for merchant %s (%d): %d kobo (wallet)", settlementID, businessName, merchantID, merchantBalance.PendingBalance)

		// Clear Redis pending data for this merchant after successful settlement
		if s.redis != nil {
			s.clearMerchantPending(ctx, strconv.Itoa(merchantID))
		}

		processedCount++
	}

	log.Printf("Settlement check complete. Processed %d settlements", processedCount)
}

// clearMerchantPending clears Redis pending settlement data for a merchant
func (s *SettlementScheduler) clearMerchantPending(ctx context.Context, merchantID string) {
	pipe := s.redis.Pipeline()

	// Remove from pending set
	pipe.SRem(ctx, "settlements:merchants:pending", merchantID)

	// Clear amount
	pipe.Del(ctx, "settlements:amounts:"+merchantID)

	// Clear transaction set
	pipe.Del(ctx, "settlements:txns:"+merchantID)

	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("Warning: Failed to clear Redis data for merchant %s: %v", merchantID, err)
	} else {
		log.Printf("Cleared Redis pending data for merchant %s", merchantID)
	}
}

// hasSettledToday checks if a settlement was already created today for this merchant
func (s *SettlementScheduler) hasSettledToday(ctx context.Context, merchantID string) bool {
	query := `
		SELECT COUNT(*) FROM settlements
		WHERE merchant_id = $1
		  AND DATE(created_at) = CURRENT_DATE
	`

	var count int
	err := s.db.QueryRowContext(ctx, query, merchantID).Scan(&count)
	if err != nil {
		log.Printf("Error checking today's settlements: %v", err)
		return false
	}

	return count > 0
}

// getMerchantBalance fetches the merchant's full balance information
func (s *SettlementScheduler) getMerchantBalance(ctx context.Context, merchantID int) (dto.MerchantBalanceResponse, error) {
	log.Printf("Getting full balance for merchant %d", merchantID)
	if s.merchantServiceURL == "" {
		return dto.MerchantBalanceResponse{}, fmt.Errorf("merchant service URL not configured")
	}

	url := fmt.Sprintf("%s/merchants/%d/balance", strings.TrimRight(s.merchantServiceURL, "/"), merchantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return dto.MerchantBalanceResponse{}, fmt.Errorf("failed to create request to merchant service: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return dto.MerchantBalanceResponse{}, fmt.Errorf("failed to get balance from merchant service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return dto.MerchantBalanceResponse{}, fmt.Errorf("merchant service returned status %d", resp.StatusCode)
	}

	var balanceResponse dto.MerchantBalanceResponse
	if err := json.NewDecoder(resp.Body).Decode(&balanceResponse); err != nil {
		return dto.MerchantBalanceResponse{}, fmt.Errorf("failed to decode balance response from merchant service: %w", err)
	}

	log.Printf("Balance for merchant %d: Available=%d kobo, Pending=%d kobo", merchantID, balanceResponse.AvailableBalance, balanceResponse.PendingBalance)

	return balanceResponse, nil
}

// createSettlement creates a new settlement record
func (s *SettlementScheduler) createSettlement(
	ctx context.Context,
	merchantID int,
	amount int64,
	currency string,
) (string, error) {
	query := `
		INSERT INTO settlements (
			merchant_id, amount, currency, status,
			bank_name, account_number, account_name,
			settled_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, NULL, NULL, NULL, NOW(), NOW(), NOW())
		RETURNING id
	`

	var settlementID string
	err := s.db.QueryRowContext(
		ctx, query,
		merchantID, amount, currency, "processing",
	).Scan(&settlementID)

	if err != nil {
		return "", fmt.Errorf("failed to create settlement: %w", err)
	}

	// Mark settlement as completed with settled_at timestamp.
	if _, err := s.db.ExecContext(ctx, `
		UPDATE settlements
		SET status = 'completed',
		    updated_at = NOW()
		WHERE id = $1
	`, settlementID); err != nil {
		return "", fmt.Errorf("failed to complete settlement %s: %w", settlementID, err) // Return error here
	}

	// Move funds from pending to available in merchant-service (synchronous so we can detect failures)
	if err := s.settleMerchantBalance(context.Background(), merchantID, currency, amount, settlementID); err != nil {
		return settlementID, fmt.Errorf("failed to settle balance in merchant-service: %w", err)
	}

	return settlementID, nil
}

func (s *SettlementScheduler) settleMerchantBalance(ctx context.Context, merchantID int, currency string, amount int64, settlementID string) error {
	if s.merchantServiceURL == "" {
		return fmt.Errorf("merchant service URL not configured")
	}
	url := fmt.Sprintf("%s/internal/balance/settle", strings.TrimRight(s.merchantServiceURL, "/"))
	payload := map[string]interface{}{
		"merchant_id": merchantID,
		"currency":    currency,
		"amount":      float64(amount) / 100, // send in currency units
		"settlement":  settlementID,
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("settlement: failed to build settle request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("settlement: failed to call merchant-service settle for merchant %d: %w", merchantID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var body bytes.Buffer
		_, _ = body.ReadFrom(resp.Body)
		return fmt.Errorf("settlement: merchant-service settle returned %d for merchant %d (settlement %s): %s", resp.StatusCode, merchantID, settlementID, strings.TrimSpace(body.String()))
	}
	log.Printf("settlement: moved %d kobo for merchant %d (settlement %s)", amount, merchantID, settlementID)
	return nil
}
