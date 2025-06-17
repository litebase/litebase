package utils

import (
	"errors"
	"math"
)

func SafeIntToInt32(i int) (int32, error) {
	if i < math.MinInt32 || i > math.MaxInt32 {
		return 0, errors.New("integer overflow: value out of int32 range")
	}

	return int32(i), nil
}

func SafeIntToUint32(i int) (uint32, error) {
	if i < 0 || i > math.MaxUint32 {
		return 0, errors.New("integer overflow: value out of uint32 range")
	}

	return uint32(i), nil
}

func SafeInt64ToInt32(i int64) (int32, error) {
	if i < math.MinInt32 || i > math.MaxInt32 {
		return 0, errors.New("integer overflow: value out of int32 range")
	}

	return int32(i), nil
}

func SafeInt32ToUint64(u int32) (uint64, error) {
	if u < 0 {
		return 0, errors.New("integer overflow: value out of uint64 range")
	}

	return uint64(u), nil
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

func SafeUint32ToInt32(u uint32) (int32, error) {
	if u > math.MaxInt32 {
		return 0, errors.New("integer overflow: value out of int32 range")
	}

	return int32(u), nil
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

func SafeUint64ToUint32(u uint64) (uint32, error) {
	if u > math.MaxUint32 {
		return 0, errors.New("integer overflow: value out of uint32 range")
	}

	return uint32(u), nil
}
