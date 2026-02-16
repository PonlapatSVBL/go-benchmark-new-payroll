package dto

type LogRecord struct {
	YearMonth                 string  `gorm:"column:year_month"`
	DomainCode                string  `gorm:"column:domain_code"`
	ChannelCode               string  `gorm:"column:channel_code"`
	Period                    string  `gorm:"column:period"`
	ExtraCodeFlag             string  `gorm:"column:extra_code_flag"`
	ImportLogFundFlag         string  `gorm:"column:import_log_fund_flag"`
	ImportLogNormalFlag       string  `gorm:"column:import_log_normal_flag"`
	ImportLogWorktimeFlag     string  `gorm:"column:import_log_worktime_flag"`
	MasterEmployeeFlag        string  `gorm:"column:master_employee_flag"`
	MasterEmployeeConfigFlag  string  `gorm:"column:master_employee_config_flag"`
	MasterCompanyConfigFlag   string  `gorm:"column:master_company_config_flag"`
	MasterBranchSsoConfigFlag string  `gorm:"column:master_branch_sso_config_flag"`
	EmployeeID                string  `gorm:"column:employee_id"`
	TotalSalaryOld            float64 `gorm:"column:total_salary_old"`
	TotalSalaryNew            float64 `gorm:"column:total_salary_new"`
	TotalVariance             float64 `gorm:"column:total_variance"`
}
