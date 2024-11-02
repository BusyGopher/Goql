package goql

import (
	"cmp"
	"context"
	"runtime"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type IQueryable[TOutput any] interface {
	Run(ctx context.Context) <-chan TOutput
	Wait(ctx context.Context) error
}

type Queryable[TInput, TOutput any] struct {
	parent    IQueryable[TInput]
	inputData []TInput
	inputChan <-chan TInput
	errorChan chan error
	fnFilter  []func(TInput) bool
	fnMap     func(TInput) []TOutput
	threads   int
}

func FromArray[TDataType any](inputData []TDataType) *Queryable[TDataType, TDataType] {
	return &Queryable[TDataType, TDataType]{
		inputData: inputData,
		inputChan: nil,
		fnFilter:  []func(TDataType) bool{},
		fnMap:     func(data TDataType) []TDataType { return []TDataType{data} },
		threads:   max(min(1, runtime.NumCPU()), 128),
	}
}

func FromChan[TDataType any](inputData <-chan TDataType) *Queryable[TDataType, TDataType] {
	return &Queryable[TDataType, TDataType]{
		inputData: nil,
		inputChan: inputData,
		fnFilter:  []func(TDataType) bool{},
		fnMap:     func(data TDataType) []TDataType { return []TDataType{data} },
		threads:   max(min(1, runtime.NumCPU()), 128),
	}
}

func (q *Queryable[TInput, TOutput]) Where(fn func(TInput) bool) *Queryable[TInput, TOutput] {
	q.fnFilter = append(q.fnFilter, fn)
	return q
}

func (q *Queryable[TInput, TOutput]) WithThreads(threads int) *Queryable[TInput, TOutput] {
	if (threads < 1) || (threads > 128) {
		panic("Threads must be between 1 and 128")
	}

	q.threads = threads
	return q
}

func (q *Queryable[TInput, TOutput]) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-q.errorChan:
		if !ok {
			return nil
		}
		return err
	}
}

func (q *Queryable[Input, TOutput]) Run(ctx context.Context) <-chan TOutput {
	eg, ctx := errgroup.WithContext(ctx)

	outputChan := make(chan TOutput)
	q.errorChan = make(chan error, 1)

	// Convert input data to input channel
	if q.inputData != nil {
		inputChan := make(chan Input, len(q.inputData))
		q.inputChan = inputChan
		eg.Go(func() error {
			defer close(inputChan)
			for _, data := range q.inputData {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case inputChan <- data:
				}
			}
			return nil
		})
	}

	// Check parent query
	if q.parent != nil {
		eg.Go(func() error {
			err := q.parent.Wait(ctx)
			return err
		})

		q.inputChan = q.parent.Run(ctx)
	}

	// Do the filter and map
	for i := 0; i < q.threads; i++ {
		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = errors.Errorf("Thread %v: panic while filtering/mapping query %v: %v", i, q, r)
				}
			}()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case data, ok := <-q.inputChan:
					if !ok {
						return nil
					}
					shouldMap := true
					for _, fnFilter := range q.fnFilter {
						if !fnFilter(data) {
							shouldMap = false
							break
						}
					}

					if !shouldMap {
						continue
					}

					for _, item := range q.fnMap(data) {
						outputChan <- item
					}
				}
			}
		})
	}

	// Monitor error group and close the channels
	go func() {
		defer close(outputChan)
		defer close(q.errorChan)
		err := eg.Wait()
		if err != nil {
			q.errorChan <- err
		}
	}()

	return outputChan
}

func (q *Queryable[TInput, TOutput]) ToArray(ctx context.Context, orderables ...IOrderable[TOutput]) ([]TOutput, error) {
	outputChan := q.Run(ctx)
	output := []TOutput{}

	for data := range outputChan {
		output = append(output, data)
	}

	err := q.Wait(ctx)

	if err != nil {
		return nil, err
	}

	OrderBy(output, orderables...)

	return output, nil
}

func Select[TInput, TOutput any](fnMap func(TInput) TOutput, q IQueryable[TInput]) *Queryable[TInput, TOutput] {
	return &Queryable[TInput, TOutput]{
		parent:    q,
		inputData: nil,
		inputChan: nil,
		fnFilter:  []func(TInput) bool{},
		fnMap:     func(item TInput) []TOutput { return []TOutput{fnMap(item)} },
		threads:   max(min(1, runtime.NumCPU()), 128),
	}
}

func SelectMany[TInput, TOutput any](fnMap func(TInput) []TOutput, q IQueryable[TInput]) *Queryable[TInput, TOutput] {
	return &Queryable[TInput, TOutput]{
		parent:    q,
		inputData: nil,
		inputChan: nil,
		fnFilter:  []func(TInput) bool{},
		fnMap:     fnMap,
		threads:   max(min(1, runtime.NumCPU()), 128),
	}
}

func Reduce[TInput, TOutput any](ctx context.Context, start TOutput, fnReduce func(TInput, TOutput) TOutput, q IQueryable[TInput]) (TOutput, error) {
	outputChan := q.Run(ctx)
	output := start

	for data := range outputChan {
		output = fnReduce(data, output)
	}

	err := q.Wait(ctx)

	return output, err
}

func GroupBy[TInput any, TKey cmp.Ordered, TValue any](ctx context.Context, keyGetter func(TInput) TKey, valueGetter func(TInput) TValue, q IQueryable[TInput]) (map[TKey][]TValue, error) {
	outputChan := q.Run(ctx)
	output := map[TKey][]TValue{}

	for data := range outputChan {
		key := keyGetter(data)
		value := valueGetter(data)
		output[key] = append(output[key], value)
	}

	err := q.Wait(ctx)

	return output, err
}
