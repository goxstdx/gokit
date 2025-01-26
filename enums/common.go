package enums

// 0:无效 1:有效
const (
	StatusValid   = 1
	StatusInvalid = 0
)

// 国家码
type CountryType string

const (
	CountryUnitedStates CountryType = "US" // 美国 USA？
)

var CountryList = []CountryType{
	CountryUnitedStates,
}

// 时区
type TypeTimezone string

const (
	TimezoneDefault TypeTimezone = "UTC"
)

var TimezoneList = []TypeTimezone{
	TimezoneDefault,
}

var CountryToTimezone = map[CountryType]TypeTimezone{}
