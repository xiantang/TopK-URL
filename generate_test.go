package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateUrl(t *testing.T)  {
	tmpName := "tmp.txt"
	clean,err := GenerateUrlFile(tmpName, 40)
	defer clean()
	var result int64
	filepath.Walk(tmpName, func(path string, f os.FileInfo, err error) error {
		result = f.Size()
		return nil
	})
	if err!= nil {
		t.Fatalf("tmp file create err")
	}

	if result < 20444 {
		t.Fatalf("file too small")
	}

}
