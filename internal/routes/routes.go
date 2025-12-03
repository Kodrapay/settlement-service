package routes

import (
	"github.com/gofiber/fiber/v2"
	"github.com/kodra-pay/settlement-service/internal/handlers"
	"github.com/kodra-pay/settlement-service/internal/services"
)

func Register(app *fiber.App, service string) {
	health := handlers.NewHealthHandler(service)
	health.Register(app)

	svc := services.NewSettlementService()
	h := handlers.NewSettlementHandler(svc)
	api := app.Group("/settlements")
	api.Post("/runs", h.CreateRun)
	api.Get("/:id", h.Get)
}
