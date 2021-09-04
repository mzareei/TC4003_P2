package mapreduce

import (
	"encoding/json"
	"log"
	"os"
)

func findKeysInFile(jobName string, reduceTaskNumber int, nMap int, intermediate map[string][]string) {
	for i := 0; i < nMap; i++ {
		file := openFile(reduceName(jobName, i, reduceTaskNumber))

		decodeFile(file, intermediate)

		file.Close()
	}
}

func decodeFile(file *os.File, intermediate map[string][]string) {
	dec := json.NewDecoder(file)

	for dec.More() {
		pair := decodePair(dec)

		intermediate[pair.Key] = append(intermediate[pair.Key], pair.Value)
	}
}

func decodePair(dec *json.Decoder) KeyValue {
	var pair KeyValue
	err := dec.Decode(&pair)

	if err != nil {
		log.Fatal(err)
	}

	return pair
}

func reduceEncode(
	merge *os.File,
	intermediate map[string][]string,
	reduceF func(key string, values []string) string,
) {
	enc := json.NewEncoder(merge)

	for k, v := range intermediate {
		kvReduced := KeyValue{k, reduceF(k, v)}

		err := enc.Encode(kvReduced)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	intermediate := make(map[string][]string) //map to save intermediate key value pairs.

	findKeysInFile(jobName, reduceTaskNumber, nMap, intermediate)
	merge := createFile(mergeName(jobName, reduceTaskNumber))
	reduceEncode(merge, intermediate, reduceF)

	err := merge.Close()
	if err != nil {
		log.Fatal(err)
	}
}
