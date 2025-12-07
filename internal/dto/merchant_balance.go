package dto

type MerchantBalanceResponse struct {
	MerchantID       int    `json:"merchant_id"`
	Currency         string `json:"currency"`
	PendingBalance   int64  `json:"pending_balance"`
	AvailableBalance int64  `json:"available_balance"`
	TotalVolume      int64  `json:"total_volume"`
}