package functions

// MapFunc is a function that takes an input and returns an output.
// The pippin use-case for it is to transform the object of one type into another type.
type MapFunc[In, Out any] func(In) Out

// MapWithErrFunc is a function that takes an input and returns an output and an error.
// The pippin use-case for it is to transform the object of one type into another type and return an error if any.
type MapWithErrFunc[In, Out any] func(In) (Out, error)

// FilterFunc is a function that takes an input and returns a boolean.
// The pippin use-case for it is to filter out the objects of one type.
type FilterFunc[In any] func(In) bool

// ReduceFunc is a function that takes two inputs and returns one output.
// The pippin use-case for it is to aggregate the objects of one type into the object of the same type with accumulated values.
type ReduceFunc[In any] func(In, In) In

// ForEachFunc is a function that takes an input and returns nothing.
// The pippin use-case for it is to perform some action on the object of one type in the form of a side effect.
type ForEachFunc[In any] func(In)

// ErrorFunc is a function that takes an error and returns nothing.
// The pippin use-case for it is to perform some action on the error in the form of a side effect, for example, logging.
//
// If you need to return a value on error, use [ErrorMapFunc].
type ErrorFunc func(error)

// ErrorMapFunc is a function that takes an error and returns an output.
// The pippin use-case for it is to perform some action on the error and return an output. For example, provide a default value.
//
// If you don't need to return a value on error, use [ErrorFunc].
type ErrorMapFunc[Out any] func(error) Out
