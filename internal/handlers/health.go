package handlers

import "github.com/gofiber/fiber/v2"

type HealthHandler struct {
	Service string
}

func NewHealthHandler(service string) *HealthHandler {
	return &HealthHandler{Service: service}
}

func (h *HealthHandler) Register(r fiber.Router) {
	r.Get("/health", h.Health)
}

func (h *HealthHandler) Health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok", "service": h.Service})
}
