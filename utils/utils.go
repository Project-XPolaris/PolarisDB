package utils

import (
	"fmt"
	"strconv"
)

func ToString(val interface{}) string {
	if val == nil {
		return ""
	}
	if str, ok := val.(string); ok {
		return str
	}
	if boolVal, ok := val.(bool); ok {
		if boolVal {
			return "1"
		}
		return "0"
	}
	if int64Val, ok := val.(int64); ok {
		return fmt.Sprintf("%d", int64Val)
	}
	// is byte array
	if byteVal, ok := val.([]byte); ok {
		return string(byteVal)
	}
	if float64Val, ok := val.(float64); ok {
		return strconv.FormatFloat(float64Val, 'f', -1, 64)
	}
	return ""
}
