package mr

import (
	"strconv"
	"strings"
	"testing"
	"unicode"
)

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}

func TestHandleResp_MapWork(t *testing.T) {

	worker := WorkerImpl{workerId: "test-1", state: 0}
	worker.mapf = Map
	worker.reducef = Reduce
	resp := WorkResponse{}
	resp.WorkType = MAP
	resp.WorkId = 0
	resp.ReduceCount = 5
	resp.FilePath = []string{"test_resources/test_input"}
	worker.handleResp(&resp)

	resp = WorkResponse{}
	resp.WorkType = REDUCE
	resp.WorkId = 0
	for _, rInput := range worker.outputPath {
		if rInput == "" {
			continue
		}
		resp.FilePath = append(resp.FilePath, rInput)
	}
	worker.handleResp(&resp)
}
