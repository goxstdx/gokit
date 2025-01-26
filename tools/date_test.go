package tools

import (
	"fmt"
	"testing"
)

func TestAddNaturalDay(t *testing.T) {
	// 1000000000000
	// fmt.Println(1000000000000 == 1e12)

	ti := GetUnixMillis()

	fmt.Println(IsMilliTimestamp(ti))
	fmt.Println(FormatTime(ti, FmtYMDHIS))
}
