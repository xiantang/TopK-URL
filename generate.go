package main

import (
	"os"
	"strconv"
)

func GenerateUrlFile(str string, num int) (func(), error) {
	f, err := os.OpenFile(str, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	defer f.Close()
	removeFile := func() {
		os.Remove(f.Name())
	}
	if err != nil {
		return removeFile, err
	} else {
		count := 0
		for count < num {
			for i := count; i >= 0; i-- {
				_, err = f.Write([]byte("https://xiantang.info/" + strconv.Itoa(count) + "\n"))
				if err != nil {
					return removeFile, err
				}
			}
			count++
		}
	}
	return removeFile, nil

}
