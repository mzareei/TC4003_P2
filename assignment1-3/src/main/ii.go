package main

import (
	"fmt"
	"mapreduce"
	"os"
	"sort"
	"strconv"
	"strings"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	results := strings.FieldsFunc(value, mapreduce.isValueALetter)

	for _, v := range results {
		res = append(res, mapreduce.KeyValue{v, document})
	}

	return
}

func createMap(values []string) map[string]bool {
	set := make(map[string]bool)

	for _, str := range values {
		set[str] = true
	}

	return set
}

func filterValues(values []string) []string {
	var results []string

	set := createMap(values)
	for value := range set {
		if set[value] == true {
			results = append(results, value)
		}
	}

	return results
}

func reduceResults(results []string) string {
	res := strconv.Itoa(len(results)) + " "

	for i, v := range results {
		if i >= 1 {
			res += ","
		}
		res += v
	}

	return res
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	results := filterValues(values)
	sort.Strings(results)

	return reduceResults(results)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}