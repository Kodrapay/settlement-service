package dto

type SettlementRunRequest struct {
	MerchantID string `json:"merchant_id"`
	PeriodFrom string `json:"period_from"`
	PeriodTo   string `json:"period_to"`
}

type SettlementResponse struct {
	ID         string `json:"id"`
	MerchantID string `json:"merchant_id"`
	Status     string `json:"status"`
}
