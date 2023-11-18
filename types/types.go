package types

import (
	"golang.org/x/exp/constraints"
)

type Tuple[A, B any] struct {
	First  A
	Second B
}

type Number interface {
	constraints.Integer | constraints.Float
}

type ComplexNumber interface {
	constraints.Complex
}

type Void struct{}

type AggregationWithCounter[T any] struct {
	Aggregation T
	Counter     int64
}

type AggregationWithCache[T comparable] struct {
	Aggregation []T
	Cache       map[T]bool
}

type AggregationWithCacheAndCounter[T comparable] struct {
	Aggregation []T
	Cache       map[T]bool
	Counter     int64
}
