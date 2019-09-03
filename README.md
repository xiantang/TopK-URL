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

* We use stream data to process very large files and use the BKDRHash function to assign different URLs to different files. But make sure the same URL in the huge file needs to appear in the same file.
* we calculate a Top100 min-heap for each file.
* we merge all the min-heap into one min-heap.

## Quickstart

1. make sure the tests pass

   `go test -v`

   ```bash
   E:\Project\TopK-URL>go test -v
   === RUN   TestBKDRHash
   --- PASS: TestBKDRHash (0.00s)
       bkdrHash_test.go:10: Pass
   === RUN   TestGenerateUrl
   --- PASS: TestGenerateUrl (0.01s)
   === RUN   TestShowTopKUrls
   --- PASS: TestShowTopKUrls (3.00s)
   === RUN   TestCreateMinHeapFromFile
   --- PASS: TestCreateMinHeapFromFile (0.10s)
   === RUN   TestMergeTwoHeap
   --- PASS: TestMergeTwoHeap (0.21s)
   === RUN   TestMinHeap
   --- PASS: TestMinHeap (0.00s)
   === RUN   TestReadFile
   --- PASS: TestReadFile (0.14s)
   === RUN   TestPartitionHandler
   --- PASS: TestPartitionHandler (0.10s)
   === RUN   TestCreatePartitionFile
   --- PASS: TestCreatePartitionFile (0.10s)
   PASS
   ok      _/E_/Project/TopK-URL   3.912s
   ```

   

2. run main.go to print out the URL of Top100

   ```
   fre: 1000 url: https://xiantang.info/999
   fre: 999 url: https://xiantang.info/998
   fre: 998 url: https://xiantang.info/997
   fre: 997 url: https://xiantang.info/996
   fre: 996 url: https://xiantang.info/995
   fre: 995 url: https://xiantang.info/994
   fre: 994 url: https://xiantang.info/993
   fre: 993 url: https://xiantang.info/992
   fre: 992 url: https://xiantang.info/991
   fre: 991 url: https://xiantang.info/990
   fre: 990 url: https://xiantang.info/989
   fre: 989 url: https://xiantang.info/988
   fre: 988 url: https://xiantang.info/987
   fre: 987 url: https://xiantang.info/986
   fre: 986 url: https://xiantang.info/985
   fre: 985 url: https://xiantang.info/984
   ```

## Coverage

```
E:\Project\TopK-URL>go test -cover
PASS
coverage: 76.8% of statements
ok      _/E_/Project/TopK-URL   4.185s
```

