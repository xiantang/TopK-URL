package main

import (
	"./utils"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestShowTopKUrls(t *testing.T) {
	CreatePartitionFile(NumFile)
	ReadFile(DataSet, SizeBatch, MapPartitionHandler)
	heap := reduce()
	urls := ShowTopKUrls(heap)
	if len(urls) != 100 {
		t.Errorf("got %d want %d", len(urls), 100)
	}

}

func TestCreateMinHeapFromFile(t *testing.T) {
	filePath := TestPath + "50.txt"
	heap := CreateHeapFromFile(filePath)
	if heap == nil {
		t.Error("create heap error")
	}
	if heap.Length() != 10 {
		t.Errorf("got %d want %d", heap.Length(), 10)
	}
}

func TestMergeTwoHeap(t *testing.T) {
	filePathA := TestPath + "50.txt"
	filePathB := TestPath + "33.txt"
	heapA := CreateHeapFromFile(filePathA)
	heapB := CreateHeapFromFile(filePathB)
	resultHeap := MergeTwoHeap(heapA, heapB)
	if resultHeap.Length() != 16 {
		t.Errorf("got %d want %d", resultHeap.Length(), TopNum)
	}
}

func TestMinHeap(t *testing.T) {
	minHeap := utils.NewMinHeap()
	minHeap.Insert(&utils.Url{Freq: 14, Addr: "baidu.com"})
	minHeap.Insert(&utils.Url{Freq: 15, Addr: "douban.com"})
	minHeap.Insert(&utils.Url{Freq: 17, Addr: "google.com"})
	url, _ := minHeap.DeleteMin()
	if !reflect.DeepEqual(url, &utils.Url{14, "baidu.com"}) {
		t.Errorf("got %s want %s", url.Addr, "baidu.com")

	}

	min, _ := minHeap.DeleteMin()
	if !reflect.DeepEqual(minHeap.Min(), &utils.Url{17, "google.com"}) {
		t.Errorf("got %s want %s", min.Addr, "baidu.com")
	}
	if minHeap.Length() != 1 {
		t.Errorf("got %d want %d", minHeap.Length(), 1)
	}
}

func RemovePartitionFile(path string, t *testing.T) {
	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			err := os.Remove(PartitionPath + f.Name())
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("file remove error %v", err)
	}
}

func TestReadFile(t *testing.T) {
	RemoveTmpFile, _ := GenerateUrlFile("tmp.txt", NumFile)
	CreatePartitionFile(NumFile)
	defer RemoveTmpFile()
	defer RemovePartitionFile(PartitionPath, t)
	err := ReadFile("tmp.txt", SizeBatch, MapPartitionHandler)
	if err != nil {
		t.Errorf("Read File error %v", err)
	}

}

func TestMapPartitionHandler(t *testing.T) {
	CreatePartitionFile(NumFile)
	//defer RemovePartitionFile(PartitionPath, t)
	memString := make([]string, 0)
	memString = append(memString, "https://xiantang.info/0")
	memString = append(memString, "https://xiantang.info/0")
	memString = append(memString, "https://xiantang.info/0")
	memString = append(memString, "https://xiantang.info/0")
	memString = append(memString, "https://xiantang.info/1")
	memString = append(memString, "https://xiantang.info/1")
	CreatePartitionFile(100)
	MapPartitionHandler(memString)
	assertFileNotNull(t)
}

func assertFileNotNull(t *testing.T) {
	success := false
	err := filepath.Walk(PartitionPath, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			if f.Size() > 0 {
				success = true
				return nil
			}
			return err
		}
		return nil
	})

	if !success {
		t.Fatalf("Read File error %v", err)
	}

}

func TestCreatePartitionFile(t *testing.T) {
	remove, _ := GenerateUrlFile("tmp", NumFile)
	CreatePartitionFile(NumFile)
	defer remove()
	defer RemovePartitionFile(PartitionPath, t)
	count := 0
	_ = filepath.Walk(PartitionPath,
		func(path string, f os.FileInfo, err error) error {
			if f == nil {
				return err
			}
			if f.IsDir() {
				return nil
			}
			count += 1
			return nil
		})
	if count != NumFile {
		t.Errorf("partition num error")
	}

}
