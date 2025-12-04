package routes

import (
	"database/sql"
	"log"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/kodra-pay/settlement-service/internal/handlers"
	"github.com/kodra-pay/settlement-service/internal/scheduler"
	"github.com/kodra-pay/settlement-service/internal/services"
	_ "github.com/lib/pq"
)

func Register(app *fiber.App, serviceName string) {
	health := handlers.NewHealthHandler(serviceName)
	health.Register(app)

	// Get database URL from environment
	dbURL := os.Getenv("POSTGRES_URL")
	if dbURL == "" {
		dbURL = "postgres://kodrapay:kodrapay_password@localhost:5432/kodrapay?sslmode=disable"
	} else {
		// Add sslmode=disable if not already present
		if !strings.Contains(dbURL, "sslmode=") {
			dbURL = dbURL + "?sslmode=disable"
		}
	}

	// Initialize database connection
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Printf("Warning: Failed to connect to database: %v. Scheduler will not run.", err)
		return
	}

	if err := db.Ping(); err != nil {
		log.Printf("Warning: Failed to ping database: %v. Scheduler will not run.", err)
		return
	}

	log.Println("Database connected successfully")

	// Initialize settlement service
	settlementSvc := services.NewSettlementService()

	// Initialize and start the settlement scheduler
	settlementScheduler := scheduler.NewSettlementScheduler(db, settlementSvc)
	settlementScheduler.Start()

	log.Println("Settlement scheduler started")
}
