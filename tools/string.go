package tools

import (
	"bytes"
	"fmt"
	"strconv"
	"unicode/utf8"

	"golang.org/x/exp/constraints"
)

// https://github.com/spf13/cast，一个三方转换包，也可以用这个

func AnyToString(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func Str2Int64(str string) (int64, error) {
	number, err := strconv.ParseInt(str, 10, 64)
	return number, err
}

func Str2Integer[T constraints.Integer](str string) (T, error) {
	number, err := Str2Int64(str)
	return T(number), err
}

func Str2IntegerMust[T constraints.Integer](str string) T {
	number, err := Str2Int64(str)
	if err != nil {
		return 0
	}

	return T(number)
}

func Integer2Str[T constraints.Integer](i T) string {
	return strconv.FormatInt(int64(i), 10)
}

func Str2Float[T constraints.Float](str string) (T, error) {
	number, err := strconv.ParseFloat(str, 64)
	return T(number), err
}

func Str2FloatMust[T constraints.Float](str string) T {
	number, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0
	}

	return T(number)
}

func Float2Str[T constraints.Float](f T) string {
	return strconv.FormatFloat(float64(f), 'f', -1, 64)
}

// Str2Bytes 将 string 转换为 []byte
func Str2Bytes(s string) []byte {
	var buf bytes.Buffer
	buf.WriteString(s)

	return buf.Bytes()
}

// Bytes2Str 将 []byte 转换为 string
func Bytes2Str(b []byte) string {
	var buf bytes.Buffer
	buf.Write(b)

	return buf.String()
}

// 把数组元素组合为字符串
// pieces : 需要转换的数组
// glue : 间隔的字符，比如逗号
// 使用方法：Implode(",",array)
func Implode(pieces []string, glue string) string {
	var buf bytes.Buffer
	l := len(pieces)
	for _, str := range pieces {
		buf.WriteString(str)
		if l--; l > 0 {
			buf.WriteString(glue)
		}
	}

	return buf.String()
}
func ImplodeMap(pieces map[string]string, glue string) string {
	var buf bytes.Buffer
	l := len(pieces)
	for _, str := range pieces {
		buf.WriteString(str)
		if l--; l > 0 {
			buf.WriteString(glue)
		}
	}
	return buf.String()
}

// GetStrLength 返回输入的字符串的字数
// 使用 utf8 字符集，任何字符都算一个，包括汉字
func GetStrLength(str string) int {
	return utf8.RuneCountInString(str)
}

// SubStrings 字符串截取，不用再考虑汉字的特殊性
func SubStrings(str string, begin, length int) (substr string) {
	// 将字符串的转换成[]rune
	rs := []rune(str)
	lth := len(rs)

	// 简单的越界判断
	if begin < 0 {
		begin = 0
	}
	if begin >= lth {
		begin = lth
	}
	end := begin + length
	if end > lth {
		end = lth
	}

	// 返回子串
	return string(rs[begin:end])
}
