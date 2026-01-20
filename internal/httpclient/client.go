package httpclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"go-benchmark-new-payroll/internal/dto"
	"net/http"
	"time"
)

var client = &http.Client{
	Timeout: 60 * time.Second,
}

func PostRequestPayroll(url string, body []byte) (int, dto.PayrollResponse, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return 0, dto.PayrollResponse{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, dto.PayrollResponse{}, err
	}
	defer resp.Body.Close()

	var result dto.PayrollResponse
	if resp.ContentLength != 0 {
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			// ตรวจสอบว่า Error ไม่ใช่ EOF (ซึ่งมักเกิดเมื่อ Response Body ว่างเปล่า)
			if err.Error() != "EOF" {
				return resp.StatusCode, dto.PayrollResponse{}, errors.New("failed to decode response body")
			}
		}
	}

	return resp.StatusCode, result, nil
}
