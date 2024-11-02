# Goql
A library that shouldn't be used which does parallel map and reduce based on .net linq library.

## Examples of usage

### Code:

```go
	ctx := context.Background()
	inputData := []int{1, 2, 3, 4, 5}

	output, _ := SelectMany(
		func(i int) []string { return []string{"a" + strconv.Itoa(i), "z" + strconv.Itoa(i)} },
		FromArray(inputData).
			WithThreads(1).
			Where(func(i int) bool { return i%2 == 0 })).
		ToArray(ctx,
			OrderDesc(func(i string) string { return i }))

	fmt.Printf("%v\n", output)
```

### Result:

```
[z4 z2 a4 a2]
```