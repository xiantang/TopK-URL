package utils

import (
	"container/list"
)

func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,

		// 链表
		linkedList: list.New(),

		hashMap: make(map[interface{}]*list.Element),
	}
}

func NewWithCallback(maxEntries int, callback func(key Key, value interface{})) *Cache {
	return &Cache{
		MaxEntries: maxEntries,

		OnEvicted: callback,
		// 链表
		linkedList: list.New(),

		hashMap: make(map[interface{}]*list.Element),
	}
}

type Key interface{}

type entry struct {
	key   Key
	value interface{}
}

// Cache 结构体，定义lru hashMap 不是线程安全的
type Cache struct {
	// 数目限制，0是无限制
	MaxEntries int

	// 删除时, 可以添加可选的回调函数
	OnEvicted func(key Key, value interface{})

	linkedList *list.List                    // 使用链表保存数据
	hashMap    map[interface{}]*list.Element // map
}

func (c Cache) Set(key Key, value interface{}) {
	if c.hashMap == nil {
		c.hashMap = make(map[interface{}]*list.Element)
		c.linkedList = list.New()
	}
	if ee, ok := c.hashMap[key]; ok {
		c.linkedList.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return
	}
	// 将数据添加到链表头部
	ele := c.linkedList.PushFront(&entry{key, value})
	c.hashMap[key] = ele
	if c.MaxEntries != 0 && c.Len() > c.MaxEntries {
		c.RemoveLast()
	}
}

// 删除最后一个元素
func (c *Cache) RemoveLast() {
	back := c.linkedList.Back()
	c.RemoveElement(back)
}

// 删除指定元素
func (c *Cache) RemoveElement(element *list.Element) {
	c.linkedList.Remove(element)
	kv := element.Value.(*entry)
	delete(c.hashMap, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

func (c *Cache) Len() int {
	if c.hashMap == nil {
		return 0
	}
	return c.linkedList.Len()
}

func (c *Cache) Get(key Key) (value interface{}, ok bool) {
	if c.hashMap == nil {
		return
	}
	if ele, hit := c.hashMap[key]; hit {
		c.linkedList.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}
