package services

import (
	"context"

	"github.com/google/uuid"

	"github.com/kodra-pay/settlement-service/internal/dto"
)

type SettlementService struct{}

func NewSettlementService() *SettlementService { return &SettlementService{} }

func (s *SettlementService) CreateRun(_ context.Context, req dto.SettlementRunRequest) dto.SettlementResponse {
	return dto.SettlementResponse{
		ID:         "stl_" + uuid.NewString(),
		MerchantID: req.MerchantID,
		Status:     "pending",
	}
}

func (s *SettlementService) Get(_ context.Context, id string) dto.SettlementResponse {
	return dto.SettlementResponse{
		ID:         id,
		MerchantID: "unknown",
		Status:     "pending",
	}
}
