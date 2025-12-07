package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kodra-pay/settlement-service/internal/services"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

type SettlementScheduler struct {
	db               *sql.DB
	redis            *redis.Client
	settlementSvc    *services.SettlementService
	ticker           *time.Ticker
	stopChan         chan bool
	checkIntervalMin int
	delayMinutes     int
}

func NewSettlementScheduler(db *sql.DB, settlementSvc *services.SettlementService) *SettlementScheduler {
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
		db:               db,
		redis:            redisClient,
		settlementSvc:    settlementSvc,
		checkIntervalMin: 1, // Check every minute for the demo
		delayMinutes:     5, // T+1 simulated as 5 minutes for demo
		stopChan:         make(chan bool),
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

	// OPTIMIZATION: Check Redis first for merchants with pending transactions
	var merchantsToCheck []string

	if s.redis != nil {
		// Get merchants from Redis pending set (only merchants with new transactions)
		pendingMerchants, err := s.redis.SMembers(ctx, "settlements:merchants:pending").Result()
		if err != nil {
			log.Printf("Warning: Failed to get pending merchants from Redis: %v", err)
			// Fall back to checking all merchants
			merchantsToCheck = nil
		} else if len(pendingMerchants) == 0 {
			log.Println("No merchants with pending transactions in Redis queue. Skipping settlement check.")
			return
		} else {
			merchantsToCheck = pendingMerchants
			log.Printf("Found %d merchants with pending transactions in Redis", len(pendingMerchants))
		}
	}

	// Get current time
	now := time.Now()
	currentTime := now.Format("15:04:05")
	dayOfWeek := int(now.Weekday())
	if dayOfWeek == 0 {
		dayOfWeek = 7 // Sunday = 7
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
			merchantID          string
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
		if s.hasSettledToday(ctx, merchantID) {
			log.Printf("Merchant %s (%s) already settled today, skipping", businessName, merchantID)
			continue
		}

		// Get merchant's available balance (excluding T+N delayed transactions)
		// Use minute-based delay for demo (T+1 simulated as 5 minutes) instead of days.
		cutoffDate := now.Add(-time.Duration(s.delayMinutes) * time.Minute)
		balance, err := s.getAvailableBalance(ctx, merchantID, cutoffDate)
		if err != nil {
			log.Printf("Error getting balance for merchant %s: %v", merchantID, err)
			continue
		}

		// Check if balance meets minimum
		if balance < minimumAmount {
			log.Printf("Merchant %s (%s) balance %d kobo below minimum %d kobo, skipping",
				businessName, merchantID, balance, minimumAmount)
			continue
		}

		// Get merchant's bank account
		bankAccount, err := s.getBankAccount(ctx, merchantID)
		if err != nil {
			log.Printf("Error getting bank account for merchant %s: %v", merchantID, err)
			continue
		}

		// Create settlement
		settlementID, err := s.createSettlement(ctx, merchantID, balance, currency, bankAccount)
		if err != nil {
			log.Printf("Error creating settlement for merchant %s: %v", merchantID, err)
			continue
		}

		log.Printf("Created settlement %s for merchant %s (%s): %d kobo to %s (%s)",
			settlementID, businessName, merchantID, balance, bankAccount.AccountName, bankAccount.AccountNumber)

		// Clear Redis pending data for this merchant after successful settlement
		if s.redis != nil {
			s.clearMerchantPending(ctx, merchantID)
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

type BankAccount struct {
	BankName      string
	AccountNumber string
	AccountName   string
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

// getAvailableBalance calculates the merchant's available balance for settlement
func (s *SettlementScheduler) getAvailableBalance(ctx context.Context, merchantID string, cutoffDate time.Time) (int64, error) {
	query := `
		SELECT COALESCE(SUM(CASE
			WHEN entry_type = 'credit' THEN amount
			WHEN entry_type = 'debit' THEN -amount
			ELSE 0
		END), 0) as balance
		FROM wallet_ledger
		WHERE merchant_id = $1
		  AND created_at <= $2
	`

	var balance int64
	err := s.db.QueryRowContext(ctx, query, merchantID, cutoffDate).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("failed to get available balance: %w", err)
	}

	// Subtract any pending settlements
	pendingQuery := `
		SELECT COALESCE(SUM(amount), 0)
		FROM settlements
		WHERE merchant_id = $1
		  AND status IN ('pending', 'processing')
	`

	var pending int64
	err = s.db.QueryRowContext(ctx, pendingQuery, merchantID).Scan(&pending)
	if err != nil {
		return 0, fmt.Errorf("failed to get pending settlements: %w", err)
	}

	return balance - pending, nil
}

// getBankAccount retrieves the merchant's primary bank account
func (s *SettlementScheduler) getBankAccount(ctx context.Context, merchantID string) (*BankAccount, error) {
	query := `
		SELECT bank_name, account_number, account_name
		FROM bank_accounts
		WHERE merchant_id = $1
		  AND status = 'active'
		ORDER BY created_at DESC
		LIMIT 1
	`

	var ba BankAccount
	err := s.db.QueryRowContext(ctx, query, merchantID).Scan(
		&ba.BankName, &ba.AccountNumber, &ba.AccountName,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no active bank account found")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get bank account: %w", err)
	}

	return &ba, nil
}

// createSettlement creates a new settlement record
func (s *SettlementScheduler) createSettlement(
	ctx context.Context,
	merchantID string,
	amount int64,
	currency string,
	bankAccount *BankAccount,
) (string, error) {
	query := `
		INSERT INTO settlements (
			merchant_id, amount, currency, status,
			bank_name, account_number, account_name,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		RETURNING id
	`

	var settlementID string
	err := s.db.QueryRowContext(
		ctx, query,
		merchantID, amount, currency, "processing",
		bankAccount.BankName, bankAccount.AccountNumber, bankAccount.AccountName,
	).Scan(&settlementID)

	if err != nil {
		return "", fmt.Errorf("failed to create settlement: %w", err)
	}

	// Debit the wallet
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO wallet_ledger (
			merchant_id, entry_type, amount, balance_after,
			currency, description, reference, created_at
		) VALUES (
			$1, 'debit', $2,
			(SELECT COALESCE(MAX(balance_after), 0) - $2 FROM wallet_ledger WHERE merchant_id = $1),
			$3, $4, $5, NOW()
		)
	`, merchantID, amount, currency,
		fmt.Sprintf("Settlement to %s", bankAccount.AccountNumber),
		fmt.Sprintf("SETTLEMENT_%s", settlementID),
	)

	if err != nil {
		return "", fmt.Errorf("failed to create wallet ledger entry: %w", err)
	}

	// Mark settlement as completed with settled_at timestamp.
	if _, err := s.db.ExecContext(ctx, `
		UPDATE settlements
		SET status = 'completed',
		    settled_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
	`, settlementID); err != nil {
		return settlementID, fmt.Errorf("failed to complete settlement %s: %w", settlementID, err)
	}

	return settlementID, nil
}
