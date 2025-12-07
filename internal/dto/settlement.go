package dto

type SettlementRunRequest struct {
	MerchantID int `json:"merchant_id"`
	PeriodFrom string `json:"period_from"`
	PeriodTo   string `json:"period_to"`
}

type SettlementResponse struct {
	ID         int    `json:"id"`
	MerchantID int    `json:"merchant_id"`
	Status     string `json:"status"`
}
