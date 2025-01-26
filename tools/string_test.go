package tools

import (
	"fmt"
	"testing"
	"unicode/utf8"
)

func TestStrLen(t *testing.T) {
	fmt.Println(CountCharacters("123你好啊，hello world!"))
	fmt.Println(GetStrLength("123你好啊，hello world!"))
}

func CountCharacters(s string) int {
	return utf8.RuneCountInString(s)
}
