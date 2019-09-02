package main

import (
	"./utils"
	"testing"
)

func TestBKDRHash(t *testing.T) {
	if utils.BKDRHash64("https://baidu.com") == uint64(3937770163576849323) {
		t.Log("Pass")
	} else {
		t.Error("Failed")
	}
}
