package main

import (
	"./utils"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMinHeap(t *testing.T) {
	minHeap := utils.NewMinHeap()
	minHeap.Insert(&utils.Url{Freq: 14, Addr: "baidu.com"})
	minHeap.Insert(&utils.Url{Freq: 15, Addr: "douban.com"})
	minHeap.Insert(&utils.Url{Freq: 17, Addr: "google.com"})
	url, _ := minHeap.DeleteMin()
	if !reflect.DeepEqual(url, &utils.Url{14, "baidu.com",}) {
		t.Errorf("got %s want %s", url.Addr, "baidu.com")

	}

	min, _ := minHeap.DeleteMin()
	if !reflect.DeepEqual(minHeap.Min(), &utils.Url{17, "google.com",}) {
		t.Errorf("got %s want %s", min.Addr, "baidu.com")
	}

}

func RemovePartitionFile(path string, t *testing.T) {
	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			err := os.Remove(PARTITION_PATH + f.Name())
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("file remove error %v", err)
	}
}

func TestReadFile(t *testing.T) {
	RemoveTmpFile, _ := GenerateUrlFile("tmp.txt", NUM_FILE)
	CreatePartitionFile(NUM_FILE)
	defer RemoveTmpFile()
	defer RemovePartitionFile(PARTITION_PATH, t)
	err := ReadFile("tmp.txt")
	if err != nil {
		t.Errorf("Read File error %v", err)
	}

}

func TestPartitionHandler(t *testing.T) {
	CreatePartitionFile(NUM_FILE)
	defer RemovePartitionFile(PARTITION_PATH, t)
	memString := make([]string, 0)
	memString = append(memString, "https://xiantang.info/0")
	memString = append(memString, "https://xiantang.info/1")
	memString = append(memString, "https://xiantang.info/4")
	memString = append(memString, "https://xiantang.info/2")
	CreatePartitionFile(100)
	PartitionHandler(memString)
	success := false

	err := filepath.Walk(PARTITION_PATH, func(path string, f os.FileInfo, err error) error {
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
	remove, _ := GenerateUrlFile("tmp", NUM_FILE)
	CreatePartitionFile(NUM_FILE)
	defer remove()
	defer RemovePartitionFile(PARTITION_PATH, t)
	count := 0
	_ = filepath.Walk(PARTITION_PATH,
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
	if count != NUM_FILE {
		t.Errorf("partition num error")
	}

}
