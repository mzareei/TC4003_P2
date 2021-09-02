package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// HINT: use for loop over `nums`
	sum := 0
	lenNums := len(nums)
	for i := 0; i < lenNums; i++ {
		sum += <-nums
	}
	out <- sum

}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// HINT: use `readInts` and `sumWorkers`
	rawContent, err := os.ReadFile(fileName)
	check(err)
	strContent := string(rawContent)
	elements, err := readInts(strings.NewReader(strContent))
	suma := 0
	check(err)
	// fmt.Print(elements)
	// HINT: used buffered channels for splitting numbers between workers
	out := make(chan int, 1)
	index := 0
	elementsSize := len(elements)
	for i := 0; i < num; i++ {
		var numbersPerChannel = elementsSize / num
		if i == num-1 {
			numbersPerChannel += elementsSize % num
		}
		nums := make(chan int, numbersPerChannel)
		for j := 0; j < numbersPerChannel && index < elementsSize; j++ {
			nums <- elements[index]
			index++
		}
		go sumWorker(nums, out)
		suma += <-out
	}
	// fmt.Println(suma)
	return suma
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
