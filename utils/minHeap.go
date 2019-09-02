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

func (H *MinHeap) DeleteMin() (*Url, error) {
	if len(H.Element) <= 1 {
		return nil, fmt.Errorf("MinHeap is empty")
	}
	minElement := H.Element[1]
	lastElement := H.Element[len(H.Element)-1]
	var i, child int
	for i = 1; i*2 < len(H.Element); i = child {
		child = i * 2
		if child < len(H.Element)-1 && H.Element[child+1].Freq < H.Element[child].Freq {
			child++
		}
		if lastElement.Freq > H.Element[child].Freq {
			H.Element[i] = H.Element[child]
		} else {
			break
		}
	}
	H.Element[i] = lastElement
	H.Element = H.Element[:len(H.Element)-1]
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
