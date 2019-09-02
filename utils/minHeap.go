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
	i := len(heap.Element)- 1
	for ; (heap.Element[i/2]).Freq > url.Freq; i /= 2 {
		heap.Element[i] = heap.Element[i/2]
	}
}




func (heap *MinHeap) Min() *Url {
	if len(heap.Element)> 1 {
		return heap.Element[1]
	}
	return nil
}

func (heap *MinHeap) DeleteMin() (*Url,error){
	if len(heap.Element) <= 1 {
		return nil, fmt.Errorf("MinHeap is empty")
	}
	minElement := heap.Element[1]
	lastElement := heap.Element[len(heap.Element)-1]
	heap.Element = heap.Element[:len(heap.Element)-1]
	heap.Element[1] = lastElement

	for i := 1; 2*i<len(heap.Element);{
		child := i* 2
		if child<len(heap.Element)-1&&heap.Element[child+1].Freq < heap.Element[child].Freq {
			child++
		}
		if heap.Element[i].Freq>heap.Element[child].Freq {
			tmp := heap.Element[i]
			heap.Element[i] = heap.Element[child]
			heap.Element[child] = tmp
			i = child
		} else {
			break
		}
	}

	return minElement, nil

}

type Url struct {
	Freq int64
	Addr string
}

func NewMinHeap() *MinHeap {
	first := &Url{math.MinInt64, "None"}
	h := &MinHeap{Element: []*Url{first,},}
	return h
}

