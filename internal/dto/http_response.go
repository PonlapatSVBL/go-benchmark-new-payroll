package dto

type PayrollResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Payload any    `json:"payload,omitempty"`
}
