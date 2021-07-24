# Wikipedia-Search-Engine

This project implements a search engine over the ~60GB Wikipedia corpus. The code consists of indexer.py and search.py. 
Both simple and multi field queries have been implemented. 

## About:
Project can be broken down into following steps:
* Building the index over given data.
* Implementing search query and getting all the pages relevant to query.
* Implementing page ranking algorithm to get K topmost relevant pages.

## How to run:
##### python3 indexer.py pathtoXMLDumpDirectory stat.txt
- This function takes as input the corpus file and creates the entire index in a field separated manner. 
- It also creates a vocabulary list and a file containg the title-id map. 
- Along with these files, it also creates the offsets for all the files.

##### python3 search.py queries.txt
* This function takes in queries.txt as argument which contains list of queries. It returns the top K(K being mentioned along with query in queries.txt) results from the Wikipedia corpus.
