package goql

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryable_SimpleToArray(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}
	output, err :=
		FromArray(inputData).
			WithThreads(1).
			ToArray(ctx)

	assert.Nil(t, err)
	assert.Equal(t, inputData, output)
}

func TestQueryable_SimpleFilterToArray(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}
	expectedData := []int{2, 4}
	output, err :=
		FromArray(inputData).
			Where(func(i int) bool { return i%2 == 0 }).
			WithThreads(1).
			ToArray(ctx)

	assert.Nil(t, err)
	assert.Equal(t, expectedData, output)
}

func TestQueryable_SimpleSelectToArray(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}
	expectedData := []string{"i2", "i4"}
	output, err :=
		Select(
			func(i int) string { return "i" + strconv.Itoa(i) },
			FromArray(inputData).
				WithThreads(1).
				Where(func(i int) bool { return i%2 == 0 })).
			ToArray(ctx)

	assert.Nil(t, err)
	assert.Equal(t, expectedData, output)
}

func TestQueryable_SimpleOrderByToArray(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}
	expectedData := []string{"i4", "i2"}
	output, err :=
		Select(
			func(i int) string { return "i" + strconv.Itoa(i) },
			FromArray(inputData).
				Where(func(i int) bool { return i%2 == 0 })).
			ToArray(ctx,
				OrderDesc(func(i string) string { return i }))

	assert.Nil(t, err)
	assert.Equal(t, expectedData, output)
}

func TestQueryable_SimpleSelectManyToArray(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}
	expectedData := []string{"z4", "z2", "a4", "a2"}
	output, err :=
		SelectMany(
			func(i int) []string { return []string{"a" + strconv.Itoa(i), "z" + strconv.Itoa(i)} },
			FromArray(inputData).
				Where(func(i int) bool { return i%2 == 0 })).
			ToArray(ctx,
				OrderDesc(func(i string) string { return i }))

	assert.Nil(t, err)
	assert.Equal(t, expectedData, output)
}

func TestQueryable_SimpleReduceToArray(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}
	expectedData := 6
	output, err :=
		Reduce(ctx, 0, func(i int, a int) int {
			return i + a
		},
			FromArray(inputData).
				Where(func(i int) bool { return i%2 == 0 }))

	assert.Nil(t, err)
	assert.Equal(t, expectedData, output)
}

func TestQueryable_SimpleGroupByToArray(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}
	expectedData := 2
	output, err :=
		GroupBy(ctx,
			func(i int) int {
				return i % 2
			},
			func(i int) int {
				return i
			},
			FromArray(inputData))

	assert.Nil(t, err)
	assert.Equal(t, expectedData, len(output))
}

func TestQueryable_Example(t *testing.T) {
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}

	output, _ := SelectMany(
		func(i int) []string { return []string{"a" + strconv.Itoa(i), "z" + strconv.Itoa(i)} },
		FromArray(inputData).
			WithThreads(1).
			Where(func(i int) bool { return i%2 == 0 })).
		ToArray(ctx,
			OrderDesc(func(i string) string { return i }))

	t.Errorf("%v\n", output)
}

func TestQueryable_FilterError(t *testing.T) {
	ctx := context.Background()
	inputData := []int{0, 1, 2, 3, 4, 5}
	_, err :=
		Select(
			func(i int) string { return "i" + strconv.Itoa(i) },
			FromArray(inputData).
				Where(func(i int) bool { return 1/i == 1 })).
			ToArray(ctx,
				OrderDesc(func(i string) string { return i }))

	assert.NotNil(t, err)
}
