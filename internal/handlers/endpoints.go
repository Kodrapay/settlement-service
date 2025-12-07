package handlers

import (
	"github.com/gofiber/fiber/v2"

	"github.com/kodra-pay/settlement-service/internal/dto"
	"github.com/kodra-pay/settlement-service/internal/services"
)

type SettlementHandler struct {
	svc *services.SettlementService
}

func NewSettlementHandler(svc *services.SettlementService) *SettlementHandler {
	return &SettlementHandler{svc: svc}
}

func (h *SettlementHandler) CreateRun(c *fiber.Ctx) error {
	var req dto.SettlementRunRequest
	if err := c.BodyParser(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}
	return c.JSON(h.svc.CreateRun(c.Context(), req))
}

func (h *SettlementHandler) Get(c *fiber.Ctx) error {
	id, err := c.ParamsInt("id")
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "Invalid settlement ID")
	}
	return c.JSON(h.svc.Get(c.Context(), id))
}
