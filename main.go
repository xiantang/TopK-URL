package main

import (
	"./utils"
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
)

var NUM_FILE = 100
var TOP_NUM = 100
var PARTITION_PATH = "./partition/"
var TEST_PATH = "./test_partition/"
var FILE_TYPE = ".txt"
var DATA_SET = "Dataset.txt"
var SIZE_BATCH = 3900000

// create a NUM_FILE number of files
func CreatePartitionFile(num int) {
	for i := 0; i < num; i++ {
		partitionName := PARTITION_PATH + strconv.Itoa(i) + FILE_TYPE
		f, err := os.OpenFile(partitionName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			fmt.Println(err.Error())
		}
		f.Close()
	}

}

// read into memory in a certain number of rows
// pass the callback function to perform operations
func ReadFile(path string, callback func([]string)) error {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	count := 0
	memString := make([]string, 0)
	for {
		line, _, err := buf.ReadLine()
		if count == SIZE_BATCH {
			callback(memString)
			memString = make([]string, 0)
			count = 0
		}
		memString = append(memString, string(line))

		if err != nil {

			if err == io.EOF {
				callback(memString)
				memString = make([]string, 0)
				count = 0
				return nil
			}
			return err
		}
		count += 1
	}

}

// the callback function logic in readFile,
// hashes the data read into memory
// to the different location subfiles
// through the Hash algorithm
func PartitionHandler(strs []string) {
	fileMap := make(map[string][]string)

	for _, str := range strs {
		if str == "" {
			continue
		}
		partition := PARTITION_PATH + strconv.Itoa(int(utils.BKDRHash64(str))%NUM_FILE) + ".txt"
		_, exists := fileMap[partition]
		if exists {
			fileMap[partition] = append(fileMap[partition], str)
		} else {
			fileMap[partition] = []string{str}
		}
	}

	for path, values := range fileMap {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			f.Close()
		} else {
			for _, v := range values {
				_, err = f.Write([]byte(v + "\n"))
			}
			f.Close()
		}
	}

}

// combine all heaps by means of a two-two merger
func reduce() *utils.MinHeap {
	heap := utils.NewMinHeap()
	for i := 0; i < NUM_FILE; i++ {
		NextHeap := CreateHeapFromFile(PARTITION_PATH + strconv.Itoa(i) + ".txt")
		heap = MergeTwoHeap(heap, NextHeap)
	}
	return heap
}

func MergeTwoHeap(oldH, newH *utils.MinHeap) *utils.MinHeap {
	if newH == nil || newH.Length() == 0 {
		return oldH
	}
	for newH.Length() != 0 {
		value, _ := newH.DeleteMin()
		if oldH.Length() < TOP_NUM {
			oldH.Insert(value)
			continue
		}
		min := oldH.Min()
		if min.Freq <= value.Freq {
			oldH.DeleteMin()
			oldH.Insert(value)
		}
	}
	return oldH
}

// create a heap from a subfile
func CreateHeapFromFile(filePath string) *utils.MinHeap {
	FreqMap := make(map[string]int64)

	addToMapFunc := func(keys []string) {
		for _, key := range keys {
			_, exists := FreqMap[key]
			if exists {
				FreqMap[key]++
			} else {
				if key != "" {
					FreqMap[key] = 1
				}
			}
		}

	}
	ReadFile(filePath, addToMapFunc)

	heap := utils.NewMinHeap()
	for key, value := range FreqMap {
		if heap.Length() < TOP_NUM {
			heap.Insert(&utils.Url{value, key})
			continue
		}
		min := heap.Min()
		if min.Freq < value {
			_, _ = heap.DeleteMin()
			heap.Insert(&utils.Url{value, key})
		}
	}

	return heap
}

func main() {
	CreatePartitionFile(NUM_FILE)
	ReadFile(DATA_SET, PartitionHandler)
	reduce()

}
