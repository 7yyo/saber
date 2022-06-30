package util

import (
	"fmt"
	"strconv"
)

func divide(a int, b int) (float64, error) {
	if r, err := strconv.ParseFloat(fmt.Sprintf("%.2f", a/b), 64); err != nil {
		return 0, err
	} else {
		return r, nil
	}
}
