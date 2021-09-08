package mapreduce

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"unicode"
)

// Debugging enabled?
const debugEnabled = false

// DPrintf will only print if the debugEnabled const has been set to true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Propagate error if it exists
func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

// Creates a file with the specified filename
// if an error occurs the error is logged to log output file/console
func createFile(fileName string) *os.File {
	file, err := os.Create(fileName)

	if err != nil {
		log.Fatal("doMap: error creating file: ", err)
	}

	return file
}

// Opens the file specified in the fileName argument
// if an error occurs the error is logged to log output file/console
func openFile(fileName string) *os.File {
	file, err := os.Open(fileName)

	if err != nil {
		log.Fatal("doMap: error opening file: ", err)
	}

	return file
}

// This function will evaluate whether the c argument is a unicode letter
// and return true if it matches a valid letter
func isValueALetter(c rune) bool {
	return !unicode.IsLetter(c)
}
