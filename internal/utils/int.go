package utils

import (
	"errors"
	"math"
)

func SafeIntToUint32(i int) (uint32, error) {
	if i < 0 || i > math.MaxUint32 {
		return 0, errors.New("integer overflow: value out of uint32 range")
	}

	return uint32(i), nil
}

func SafeInt64ToUint32(i int64) (uint32, error) {
	if i < 0 || i > math.MaxUint32 {
		return 0, errors.New("integer overflow: value out of uint32 range")
	}

	return uint32(i), nil
}

func SafeInt64ToUint64(i int64) (uint64, error) {
	if i < 0 {
		return 0, errors.New("integer overflow: value out of uint64 range")
	}

	return uint64(i), nil
}

func SafeUint32ToInt64(u uint32) (int64, error) {
	if u > math.MaxInt32 {
		return 0, errors.New("integer overflow: value out of int64 range")
	}

	return int64(u), nil
}

func SafeUint64ToInt64(u uint64) (int64, error) {
	if u > math.MaxInt64 {
		return 0, errors.New("integer overflow: value out of int64 range")
	}

	return int64(u), nil
}
