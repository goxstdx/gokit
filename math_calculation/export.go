package math_calculation

import (
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/croe"
)

func SetLexerCacheCapacity(capacity int) {
	croe.SetLexerCacheCapacity(capacity)
}
func SetExprCacheCapacity(capacity int) {
	croe.SetExprCacheCapacity(capacity)
}
