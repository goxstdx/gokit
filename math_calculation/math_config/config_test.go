package math_config

import (
	"testing"
	"time"
)

func TestGetDefaultCalcConfig(t *testing.T) {
	config := NewDefaultCalcConfig()

	// 检查默认配置的值
	expectedConfig := &CalcConfig{
		MaxRecursionDepth:      100,
		Timeout:                time.Second * 5,
		Precision:              10,
		ApplyPrecisionEachStep: true,
		UseExprCache:           true,
		UseLexerCache:          true,
	}

	if config.MaxRecursionDepth != expectedConfig.MaxRecursionDepth {
		t.Errorf("NewDefaultCalcConfig().MaxRecursionDepth = %v, want %v", config.MaxRecursionDepth, expectedConfig.MaxRecursionDepth)
	}

	if config.Timeout != expectedConfig.Timeout {
		t.Errorf("NewDefaultCalcConfig().Timeout = %v, want %v", config.Timeout, expectedConfig.Timeout)
	}

	if config.Precision != expectedConfig.Precision {
		t.Errorf("NewDefaultCalcConfig().Precision = %v, want %v", config.Precision, expectedConfig.Precision)
	}

	if config.ApplyPrecisionEachStep != expectedConfig.ApplyPrecisionEachStep {
		t.Errorf("NewDefaultCalcConfig().ApplyPrecisionEachStep = %v, want %v", config.ApplyPrecisionEachStep, expectedConfig.ApplyPrecisionEachStep)
	}

	if config.UseExprCache != expectedConfig.UseExprCache {
		t.Errorf("NewDefaultCalcConfig().UseExprCache = %v, want %v", config.UseExprCache, expectedConfig.UseExprCache)
	}

	if config.UseLexerCache != expectedConfig.UseLexerCache {
		t.Errorf("NewDefaultCalcConfig().UseLexerCache = %v, want %v", config.UseLexerCache, expectedConfig.UseLexerCache)
	}
}
