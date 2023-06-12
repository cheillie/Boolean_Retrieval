# Boolean Retrieval

## Indexing
Documents to be indexed are stored in directory-of-documents. In this homework, we are going to use the Reuters training data set provided by NLTK. 
.../nltk_data/corpora/reuters/training/

Indexing script, `index.py` should be called in this format\
 `python3 index.py -i directory-of-documents -d dictionary-file -p postings-file`

## Searching
The command to run your searching script, search.py: 

`python3 search.py -d dictionary-file -p postings-file -q file-of-queries -o output-file-of-results`\
dictionary-file and postings-file are the output files from the indexing phase. Queries to be tested are stored in file-of-queries, in which one query occupies one line.


### Python Version

We're using Python Version 3.8.10 for this assignment.

### General Notes about this assignment


index.py:
Here are my indexing steps:
1) parse all documents from in_dir
    - these are parsed in increasing order to ensure this property is propagated to the postings lists 
2) construct a partitioned inverted index from the document tokens
    - I experimented with different partition and chunk sizes. After generating the index without partitioning,
    I found that there were roughly 35000 unique terms. Thus, I chose a partition size of 3500, hoping for 
    roughly 10 partitions. However, including all the intermediate blocks created while merging, I ended up with
    nearly 90 partitions.
3) merge the partitions
    - I experimented a lot here with optimizing the merging algorithm. I thought that maintaining
    a sort throughout the whole indexing would allow me to simply append postings lists to one another
    when performing the merge. However, I could not figure out how to accomplish this in time, and ended up
    using heapq.merge() to combine the already sorted postings. 
4) add a list of all docIDs to the index to facilitate NOT queries
    - a single NOT query requires the inversion of the postings list, which means we must provide search.py
    with a list of all document IDs from which the index was created. 
5) add skip pointers to the index
    - Skip intervals are the square root of the length of the postings list.
    Based on this calculation, postings lists that contain at least 16 documents 
    can benefit from skip pointers. Anything less would result in equal or more
    work than simply parsing the list linearly. 

search.py:
1) the queries_file is read line by line
2) if the query is longer than 1024, an empty result line will be logged to the results_file
3) parse the query using shunting yard algorithm into Reverse Polish Notation
    - see https://www.youtube.com/watch?v=Jd71l0cHZL0 for details of the algorithm
4) evalute the query based on whether if its a operator (AND, OR, NOT) or operand
5) AND operator: the intersect algorithm is implemented based on how we went over in lecture
    - Case 1: the docIDs of p1 and p2 match, append (docID, skip pointer) tuple to the answer list,
    and advance the index for p1 and p2
    - Case 2: docID(p1) is less than docID(p2). Repeatedly check if the skip pointer of docID(p1) is
    less than or equal to docID(p2). Advance p1 to the final skip pointer if skippable, othereise just
    advance p1 by one 
    - Case 3: docID(p2) is less than docID(p1). Same logic as Case 2
6) OR operator: the OR algorithm does not use skip pointer. But we need to follow a similar approach
to ensure that the result docID is sequential
    - Case 1: the docIDs of p1 and p2 match, append (docID, skip pointer) p1 tuple to the answer list
    and advance both indexes
    - Case 2: docID(p1) is less than docID(p2). Append (docID, skip pointer) p1 tuple to the answer list
    and advance only p1 index
    - Case 3: docID(p2) is less than docID(p1). Same logic as Case 2
    - Additional cases to optimize merge:
        - if p1 reaches the end first, append the rest of p2 postings to the answer list
        - if p2 reaches the end first, append the rest of p1 postings to the answer list
7) NOT operator: the NOT algorithm also does not utilize the skip pointer.
    - Case 1: if the posting for a term is empty, that means ALL postings are the result
    - Case 2: go through every single document and only append the docID that does not exist in p1
8) Lastly, parse the tuple (docID, skip pointer) into just the docID and write it to the results_file

### Files included with this submission
README.txt: current file, contains information for this assignment\
index.py: Create index from the given documents\
search.py: The main searching algorithm\
dictionary.txt: The first line is a list of all document IDs, the rest of lines consist of (term, document frequency, pointer to the posting)\
postings.txt: each line is a (docID, skip pointer index) tuple

### References

I refered to https://www.youtube.com/watch?v=Jd71l0cHZL0 for the implementation of 
the shunting yard algorithm in search.py
