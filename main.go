package main

import (
	"./utils"
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var NumFile = 100
var TopNum = 100
var PartitionPath = "./partition/"
var TestPath = "./test_partition/"
var FileType = ".txt"
var DataSet = "Dataset.txt"
var SizeBatch = 3900000

// create a NumFile number of files
func CreatePartitionFile(num int) {
	for i := 0; i < num; i++ {
		partitionName := PartitionPath + strconv.Itoa(i) + FileType
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
		if count == SizeBatch {
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
// to the different location sub files
// through the Hash algorithm
func PartitionHandler(strs []string) {
	fileMap := make(map[string][]string)

	for _, str := range strs {
		if str == "" {
			continue
		}
		partition := PartitionPath + strconv.Itoa(int(utils.BKDRHash64(str))%NumFile) + ".txt"
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

func MapPartitionHandler(strs []string) {

	fileMap := make(map[string]int64)
	for _, str := range strs {
		if str == "" {
			continue
		}
		_, exists := fileMap[str]
		if exists {
			fileMap[str] += 1
		} else {
			fileMap[str] = 1
		}
	}

	for url, num := range fileMap {
		path := PartitionPath + strconv.Itoa(int(utils.BKDRHash64(url))%NumFile) + ".txt"
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		partitionMap := make(map[string]int64)
		callback := func(strs []string) {
			for _, str := range strs {
				if str == "" {
					continue
				}
				s := strings.Split(str, "  ")
				partitionUrl := s[0]
				partitionNum, _ := strconv.ParseInt(s[1], 10, 64)
				partitionMap[partitionUrl] = partitionNum
			}
		}
		ReadFile(path, callback)
		value, exists := partitionMap[url]
		if exists {
			partitionMap[url] = value + num
		} else {
			partitionMap[url] = num
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
		if err != nil {
			fmt.Println("file create failed. err: " + err.Error())
		} else {
			f.Seek(0, 0)
			result := ""
			for url, num := range partitionMap {
				result += url + "  " + strconv.FormatInt(num, 10) + "\n"
			}
			f.Write([]byte(result))
			defer f.Close()
		}
	}
}

// combine all heaps by means of a two-two merger
func reduce() *utils.MinHeap {
	heap := utils.NewMinHeap()
	for i := 0; i < NumFile; i++ {
		NextHeap := CreateHeapFromFile(PartitionPath + strconv.Itoa(i) + ".txt")
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
		if oldH.Length() < TopNum {
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

// create a heap from a sub file
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
		if heap.Length() < TopNum {
			heap.Insert(&utils.Url{Freq: value, Addr: key})
			continue
		}
		min := heap.Min()
		if min.Freq < value {
			_, _ = heap.DeleteMin()
			heap.Insert(&utils.Url{Freq: value, Addr: key})
		}
	}

	return heap
}

func ShowTopKUrls(heap *utils.MinHeap) []*utils.Url {
	length := heap.Length()
	urls := make([]*utils.Url, length)
	for i := length - 1; i >= 0; i-- {
		url, _ := heap.DeleteMin()
		urls[i] = url
	}
	return urls
}

func RemoveFiles(path string) {
	filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			err := os.Remove(PartitionPath + f.Name())
			return err
		}
		return nil
	})

}

func main() {
	CreatePartitionFile(NumFile)
	//ReadFile(DataSet, MapPartitionHandler)

	ReadFile(DataSet, PartitionHandler)
	//heap := reduce()
	//urls := ShowTopKUrls(heap)
	//defer RemoveFiles(PartitionPath)
	//for _, url := range urls {
	//	fmt.Printf("fre: %d url: %s \n", url.Freq, url.Addr)
	//}

}
