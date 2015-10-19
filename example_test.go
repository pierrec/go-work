package work_test

import (
	"fmt"

	"github.com/pierrec/go-work"
)

func Example_numbers() {
	// Process a list of numbers by doubling each number
	list := []int{1, 2, 4, 5}         // the input list of numbers
	results := make([]int, len(list)) // the output

	// worker does the doubling
	worker := func(idx int) {
		results[idx] = list[idx] * 2
	}

	// finalizer prints the result, preserving the list ordering
	finalizer := func(idx int) {
		fmt.Printf("%d\n", results[idx])
	}

	work.Do(len(list), worker, finalizer)
	// Output:
	// 2
	// 4
	// 8
	//10
}
