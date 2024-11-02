package goql

import (
	"cmp"
	"slices"
)

type IOrderable[TInput any] interface {
	GetOrderFn(TInput, TInput) int
}

type Orderable[TInput any, TOutput cmp.Ordered] struct {
	keyGetter func(TInput) TOutput
	desc      bool
}

func OrderAsc[TInput any, TOutput cmp.Ordered](keyGetter func(TInput) TOutput) IOrderable[TInput] {
	return &Orderable[TInput, TOutput]{keyGetter: keyGetter, desc: false}
}

func OrderDesc[TInput any, TOutput cmp.Ordered](keyGetter func(TInput) TOutput) IOrderable[TInput] {
	return &Orderable[TInput, TOutput]{keyGetter: keyGetter, desc: true}
}

func (o *Orderable[TInput, TOutput]) GetOrderFn(a TInput, b TInput) int {
	akey := o.keyGetter(a)
	bkey := o.keyGetter(b)

	if o.desc {
		return cmp.Compare(bkey, akey)
	} else {
		return cmp.Compare(akey, bkey)
	}
}

func GetOrderByFunc[TInput any](orderables ...IOrderable[TInput]) func(TInput, TInput) int {
	return func(a, b TInput) int {
		for _, orderable := range orderables {
			c := orderable.GetOrderFn(a, b)
			if c != 0 {
				return c
			}
		}
		return 0
	}
}

func OrderBy[TInput any](data []TInput, orderables ...IOrderable[TInput]) {
	if len(orderables) > 0 && len(data) > 0 {
		slices.SortFunc(data, GetOrderByFunc(orderables...))
	}
}
