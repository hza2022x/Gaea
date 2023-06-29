package merger

import (
	"fmt"
	"strconv"
)

// GetInt get int value from column
// copy from Resultset.GetInt()
func (r ResultRow) GetInt(column int) (int64, error) {
	d := r[column]
	switch v := d.(type) {
	case uint64:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("data type is %T", v)
	}
}

// GetUint get uint64 value from column
func (r ResultRow) GetUint(column int) (uint64, error) {
	d := r[column]
	switch v := d.(type) {
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("data type is %T", v)
	}
}

// GetFloat get float64 value from column
func (r ResultRow) GetFloat(column int) (float64, error) {
	d := r[column]
	switch v := d.(type) {
	case float64:
		return v, nil
	case uint64:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("data type is %T", v)
	}
}

// SetValue set value to column
func (r ResultRow) SetValue(column int, value interface{}) {
	r[column] = value
}

// GetValue get value from column
func (r ResultRow) GetValue(column int) interface{} {
	return r[column]
}
