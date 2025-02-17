package tools

import (
	"errors"
	"fmt"
	"time"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/enums"
)

var (
	_timezone string
)

func init() {
	_timezone = string(enums.TimezoneDefault)
}

func SetTimezone(timezone enums.TypeTimezone) error {
	if !InSlice(timezone, enums.TimezoneList) {
		return errors.New("unknown timezone")
	}

	loc, err := time.LoadLocation(string(timezone))
	if err != nil {
		return errors.New(fmt.Sprintf("invalid service timezone: %v", timezone))
	}
	// 设置local为设置的时区
	// TODO 这里会导致覆盖系统变量 TZ 所设置的时区（只是go的）
	time.Local = loc

	_timezone = string(timezone)

	return nil
}

type DateFormat string

const (
	YmdFormat    DateFormat = "20060102"
	FmtYMDHI     DateFormat = "2006-01-02 15:04"
	FmtYMD       DateFormat = "2006-01-02"
	FmtYMDHIS    DateFormat = "2006-01-02 15:04:05"
	FmtYMDHISUTC DateFormat = "2006-01-02 15:04:05 UTC"
	FmtYMDHISF   DateFormat = "2006-01-02 15:04:05.9999"
	FmtYMDHISFZ  DateFormat = "2006-01-02T15:04:05.999Z"
	FmtMDYHM     DateFormat = "1/2/06 15:04"
	FmtMDMY      DateFormat = "02/01/2006"
	// 日月年时分秒
	DmyHis      DateFormat = "02012006150405"
	YmdHis      DateFormat = "20060102150405"
	YmdTHisZone DateFormat = "2006-01-02T15:04:05-07:00"
)

const (
	DayTime = time.Hour * 24
)

func IsMilliTimestamp(timestamp int64) bool {
	// 如果时间戳小于1000000000000(1970年1月1日 00:00:00 UTC),则认为是秒级时间戳
	return timestamp > 1e12
}

// GetUnixMillis 取当前系统时间的毫秒
func GetUnixMillis() int64 {
	return time.Now().UnixMilli()
}

// GetUnixMillis 取当前系统时间的毫秒
func GetNowTimeObj() time.Time {
	return time.Now()
}

func GetTimeObj(timestamp int64) time.Time {
	if IsMilliTimestamp(timestamp) {
		return time.UnixMilli(timestamp)
	} else {
		return time.Unix(timestamp, 0)
	}
}

func FormatTime(timestamp int64, format DateFormat) string {
	if timestamp <= 0 {
		return ""
	}

	return GetTimeObj(timestamp).Format(string(format))
}

func GetNowFormatTime(format DateFormat) string {
	return FormatTime(time.Now().Unix(), format)
}

func ParseTimeToTimestamp(dates string, format DateFormat) int64 {
	parse, err := time.Parse(string(format), dates)
	if err != nil {
		return 0
	}
	return parse.Unix()
}

func ParseTimeToTimestampMilli(dates string, format DateFormat) int64 {
	parse, err := time.Parse(string(format), dates)
	if err != nil {
		return 0
	}
	return parse.UnixMilli()
}

// AddNaturalDay 根据配置的时区时间,自然日自增
func AddNaturalDay(offset int) int64 {
	now := time.Now()

	// 将时间设置为当天 0 点
	newTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	// 向前或向后偏移指定的天数
	newTime = newTime.AddDate(0, 0, offset)

	return newTime.UnixMilli()
}

// AddDay 根据配置的时区时间,当前时间天自增
func AddDay(offset int) int64 {
	return time.Now().AddDate(0, 0, offset).UnixMilli()
}
func FormatDateByMilli(timestamp int64, format string) string {
	tmp := timestamp / 1000
	if tmp <= 0 {
		return ""
	}
	tm := time.Unix(tmp, 0)
	local, _ := time.LoadLocation(_timezone)

	return tm.In(local).Format(format)
}

// GetLocalDateFormatBySecTimestamp 以秒时间戳获取当地格式化时间
func GetLocalDateFormatBySecTimestamp(timestamp int64, format string) string {

	tm := time.Unix(timestamp, 0)
	local, _ := time.LoadLocation(_timezone)

	return tm.In(local).Format(format)
}

func TimeNow() int64 {
	return time.Now().Unix()
}
