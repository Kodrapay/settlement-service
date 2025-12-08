package services

import (
	"context"

	"github.com/kodra-pay/settlement-service/internal/dto"
)

type SettlementService struct{}

func NewSettlementService() *SettlementService { return &SettlementService{} }

func (s *SettlementService) CreateRun(_ context.Context, req dto.SettlementRunRequest) dto.SettlementResponse {
	// In a real scenario, this would generate a unique int ID.
	// For this mock implementation, we return a placeholder.
	// req.MerchantID (int) is now available in the request.
	return dto.SettlementResponse{
		ID:         1, // Placeholder for an auto-generated int ID
		MerchantID: req.MerchantID,
		Status:     "pending",
	}
}

func (s *SettlementService) Get(_ context.Context, id int) dto.SettlementResponse {
	return dto.SettlementResponse{
		ID:         id,
		MerchantID: 0,  // Placeholder for unknown int MerchantID
		Status:     "pending",
	}
}
