package main

import (
	"./utils"
	"testing"
)

func TestCacheSet(t *testing.T) {
	lruCache := utils.New(10)
	lruCache.Set("xiantang.info", 1)
	size := lruCache.Len()
	if size != 1 {
		t.Errorf("got %d want %d", size, 1)
	}
}

func TestAddLRUCacheWhenFull(t *testing.T) {
	lruCache := utils.New(3)
	lruCache.Set("xiantang.info", 1)
	lruCache.Set("xiantang.info1", 1)
	lruCache.Set("xiantang.info2", 1)
	lruCache.Set("xiantang.info3", 1)
	size := lruCache.Len()
	if size != 3 {
		t.Errorf("got %d want %d", size, 3)
	}
	_, ok := lruCache.Get("xiantang.info")
	if ok {
		t.Errorf("got %d want %d", 0, 1)
	}
}

func TestAddDuplicateLRUCacheWhenFull(t *testing.T) {
	lruCache := utils.New(3)
	lruCache.Set("xiantang.info", 1)
	lruCache.Set("xiantang.info1", 1)
	lruCache.Set("xiantang.info2", 1)
	lruCache.Set("xiantang.info", 2)
	size := lruCache.Len()
	if size != 3 {
		t.Errorf("got %d want %d", size, 3)
	}
	result, _ := lruCache.Get("xiantang.info")
	if result != 2 {
		t.Errorf("got %d want %d", result, 2)
	}

}

func TestGetLRUCache(t *testing.T) {
	lruCache := utils.New(3)
	lruCache.Set("xiantang.info", 1)
	lruCache.Set("xiantang.info1", 1)
	lruCache.Set("xiantang.info2", 1)
	lruCache.Set("xiantang.info", 2)
	result, _ := lruCache.Get("xiantang.info")
	if result != 2 {
		t.Errorf("got %d want %d", result, 2)
	}
}
