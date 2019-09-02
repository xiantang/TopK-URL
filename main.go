package main

import (
	"./utils"
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

var NUM_FILE = 100
var PARTITION_PATH = "./partition/"
var FILE_TYPE = ".txt"
var DATA_SET  = "Dataset.txt"
var SIZE_BATCH = 3900000

func PartitionHandler(strs []string)  {
	fileMap := make(map[string][]string)

	for _,str := range strs {
		if str == "" {
			continue
		}
		partition := PARTITION_PATH + strconv.Itoa(int(utils.BKDRHash64(str))%NUM_FILE) + ".txt"
		_,exists  := fileMap[partition]
		if exists {
			fileMap[partition]= append(fileMap[partition], str)
		} else{
			fileMap[partition] = []string{str}
		}
	}


	for path,values := range fileMap {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			f.Close()
		} else{
			for _, v := range values {
				_, err = f.Write([]byte(v + "\n"))
			}
			f.Close()
		}

	}


}


func ReadFile(path string) error{
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	count := 0
	memString := make([]string, 0)

	t1 := time.Now()
	for  {
		line, _, err := buf.ReadLine()
		if count == SIZE_BATCH {
			PartitionHandler(memString)
			memString = make([]string, 0)
			count = 0
		}
		memString = append(memString, string(line))
		if err != nil {
			if err == io.EOF {
				PartitionHandler(memString)
				memString = make([]string, 0)
				count = 0
				t2 := time.Now()
				fmt.Println("spend time:", t2.Sub(t1))
				return nil
			}
			return err
		}
		count+= 1
	}



}

func CreatePartitionFile(num int)  {
	for i := 0; i < num; i++ {
		partitionName := PARTITION_PATH  + strconv.Itoa(i)+ FILE_TYPE
		f, err := os.OpenFile(partitionName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			fmt.Println(err.Error())
		}
		f.Close()
	}

}

func main() {
	CreatePartitionFile(NUM_FILE)
	ReadFile(DATA_SET)

}