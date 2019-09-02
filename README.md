# TopK-URL

## Introduction

Assuming the file size is 100G, the URL file used by this project is taken as an example. We need to find the top K URLs that appear most frequently in all URLs on a machine with only 1G of RAM.

## File structure

analyze a 100GB link using Min Heap + Hash to get a Demo with a Top100 link

```tree
TopK-URL
│  bkdrHash_test.go
│  Dataset.txt
│  generate.go
│  generate_test.go
│  main.go
│  topk_url_test.go
│
│
├─partition
├─test_partition
│      33.txt
│      50.txt
│
└─utils
```

## Implement

- We use stream data to process very large files and use the BKDRHash function to assign different URLs to different files. But make sure the same URL in the huge file needs to appear in the same file.
- we calculate a Top100 min-heap for each file.
- we merge all the min-heap into one min-heap.