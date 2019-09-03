package utils

import (
	"fmt"
	"math"
)

type MinHeap struct {
	Element []*Url
}

func (heap *MinHeap) Insert(url *Url) {
	// use floating operation to maintain heap order
	heap.Element = append(heap.Element, url)
	i := len(heap.Element) - 1
	for ; (heap.Element[i/2]).Freq > url.Freq; i /= 2 {
		heap.Element[i] = heap.Element[i/2]
	}
	heap.Element[i] = url
}

func (heap *MinHeap) Length() int {
	return len(heap.Element) - 1
}

func (heap *MinHeap) Min() *Url {
	if len(heap.Element) > 1 {
		return heap.Element[1]
	}
	return nil
}

func (heap *MinHeap) DeleteMin() (*Url, error) {
	if len(heap.Element) <= 1 {
		return nil, fmt.Errorf("MinHeap is empty")
	}
	minElement := heap.Element[1]
	lastElement := heap.Element[len(heap.Element)-1]
	var i, child int
	for i = 1; i*2 < len(heap.Element); i = child {
		child = i * 2
		if child < len(heap.Element)-1 && heap.Element[child+1].Freq < heap.Element[child].Freq {
			child++
		}
		if lastElement.Freq > heap.Element[child].Freq {
			heap.Element[i] = heap.Element[child]
		} else {
			break
		}
	}
	heap.Element[i] = lastElement
	heap.Element = heap.Element[:len(heap.Element)-1]
	return minElement, nil

}

type Url struct {
	Freq int64
	Addr string
}

func NewMinHeap() *MinHeap {
	first := &Url{math.MinInt64, "None"}
	h := &MinHeap{Element: []*Url{first}}
	return h
}
