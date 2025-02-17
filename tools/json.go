package tools

import (
	"encoding/json"
)

func ToJsonStr(obj interface{}) string {
	b, err := json.Marshal(obj)
	if err != nil {
		return ""
	}

	return string(b)
}

func ToJsonByte(obj interface{}) []byte {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil
	}

	return b
}
