package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)



// Gets file information from current opened file
// if an error occurs the error is logged to log output file/console
func getFileInfo(inputFile *os.File) os.FileInfo {
	fileInfo, err := inputFile.Stat()

	if err != nil {
		log.Fatal("doMap: error on getstat: ", err)
	}

	return fileInfo
}

// Will read and return all the file contents
// if an error occurs the error is logged to log output file/console
func readFileContents(inputFile *os.File) []byte {
	fileContents := make([]byte, getFileInfo(inputFile).Size())

	_, err := inputFile.Read(fileContents)
	if err != nil {
		log.Fatal("doMap: error reading file: ", err)
	}

	return fileContents
}

func mapLoop(
	jobName string,
	mapTaskNumber int,
	keyValues []KeyValue,
	nReduce int,
) {
	for reduceTask := 0; reduceTask < nReduce; reduceTask++ {
		reduceFile := createFile(reduceName(jobName, mapTaskNumber, reduceTask))

		encodeValuesFromReduceTask(reduceFile, keyValues, nReduce, reduceTask)

		reduceFile.Close()
	}
}

func encodeValuesFromReduceTask(
	reduceFile *os.File,
	keyValues []KeyValue,
	nReduce int,
	currentReduceTask int,
) {
	enc := json.NewEncoder(reduceFile)

	for _, kv := range keyValues {
		keyHash := int(ihash(kv.Key))
		if keyHash % nReduce == currentReduceTask {
			encodeValue(enc, &kv)
		}
	}
}

func encodeValue(enc *json.Encoder, kv *KeyValue) {
	err := enc.Encode(kv)

	if err != nil {
		log.Fatal("doMap: json encode error: ", err)
	}
}

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	file := openFile(inFile)

	keyValues := mapF(inFile, string(readFileContents(file)))
	mapLoop(jobName, mapTaskNumber, keyValues, nReduce)

	err := file.Close()
	if err != nil {
		log.Fatal("doMap: error closing file: ", err)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
