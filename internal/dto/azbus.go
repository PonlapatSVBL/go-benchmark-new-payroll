package dto

import "fmt"

type PayrollMessage struct {
	Compgrp                 string `json:"_compgrp"`
	Comp                    string `json:"_comp"`
	Action                  string `json:"_action"`
	YearMonth               string `json:"year_month"`
	EmployeeID              string `json:"employee_id"`
	CalculatePerson         string `json:"calculate_person"`
	CalculateTo             string `json:"calculate_to"`
	InstanceServerID        string `json:"instance_server_id"`
	InstanceServerChannelID string `json:"instance_server_channel_id"`
	IdentifyUserID          string `json:"identify_user_id"`
	LanguageCode            string `json:"language_code"`
	UserName                string `json:"user_name"`
	UserPsw                 string `json:"user_psw"`
	Url                     string `json:"url"`
	Schema                  string `json:"schema,omitempty"`
}

func (s *PayrollMessage) Validate() error {
	if s.Compgrp == "" {
		return fmt.Errorf("compgrp is required")
	}
	if s.Comp == "" {
		return fmt.Errorf("comp is required")
	}
	if s.Action == "" {
		return fmt.Errorf("action is required")
	}
	if s.YearMonth == "" {
		return fmt.Errorf("year_month is required")
	}
	if s.EmployeeID == "" {
		return fmt.Errorf("employee_id is required")
	}
	if s.CalculatePerson == "" {
		return fmt.Errorf("calculate_person is required")
	}
	if s.CalculateTo == "" {
		return fmt.Errorf("calculate_to is required")
	}
	if s.InstanceServerID == "" {
		return fmt.Errorf("instance_server_id is required")
	}
	if s.InstanceServerChannelID == "" {
		return fmt.Errorf("instance_server_channel_id is required")
	}
	if s.IdentifyUserID == "" {
		return fmt.Errorf("identify_user_id is required")
	}
	if s.LanguageCode == "" {
		return fmt.Errorf("language_code is required")
	}
	if s.Url == "" {
		return fmt.Errorf("url is required")
	}
	if s.Schema == "" {
		return fmt.Errorf("schema is required")
	}
	return nil
}

type CompareMessage struct {
	DbName     string `json:"db_name"`
	EmployeeID string `json:"employee_id"`
	YearMonth  string `json:"year_month"`
}

func (s *CompareMessage) Validate() error {
	if s.DbName == "" {
		return fmt.Errorf("db_name is required")
	}
	if s.EmployeeID == "" {
		return fmt.Errorf("employee_id is required")
	}
	if s.YearMonth == "" {
		return fmt.Errorf("year_month is required")
	}
	return nil
}
