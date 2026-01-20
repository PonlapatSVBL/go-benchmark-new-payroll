package dto

type LogRecord struct {
	YearMonth      string  `gorm:"column:year_month"`
	DomainCode     string  `gorm:"column:domain_code"`
	ChannelCode    string  `gorm:"column:channel_code"`
	Period         string  `gorm:"column:period"`
	ExtraCodeFlag  string  `gorm:"column:extra_code_flag"`
	EmployeeID     string  `gorm:"column:employee_id"`
	TotalSalaryOld float64 `gorm:"column:total_salary_old"`
	TotalSalaryNew float64 `gorm:"column:total_salary_new"`
	TotalVariance  float64 `gorm:"column:total_variance"`
}
