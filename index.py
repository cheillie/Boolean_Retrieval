#!/usr/bin/python3
import getopt
import linecache
import nltk
import os
import re
import shutil
import sys

from collections import deque
from heapq import merge
from itertools import islice
from math import floor, sqrt
from typing import Dict, Deque

TEST_MODE = False
VERBOSE = False
AUXILIARY_DICT = 'd'
AUXILIARY_POST = 'p'

def usage():
    print("usage: " + sys.argv[0] + " -i directory-of-documents -d dictionary-file -p postings-file -t (optional flag for test mode)")


class DictionaryEntry:
    '''
    Represents a term dictionary entry that contains document frequency 
    and the pointer to associated postings list in the postings file
    '''
    def __init__(self, postings_address, doc_freq=1):
        self.doc_freq = doc_freq
        self.postings = postings_address
        
class Index:
    '''
    Inverted index data structure containing a dictionary of terms and the associated postings lists. 
    '''
    def __init__(self):
        ''' 
        Constructor
        '''
        self.term_dictionary: Dict[str, DictionaryEntry] = {}
        self.postings: [str] = []
        
    def __len__(self):
        '''
        override built-in length function to help track block size
        '''
        return len(self.term_dictionary)
        
    def insert(self, term:str, doc_ID:str, doc_freq:int=1):
        '''
        Insert a given term into the index with the document ID from which it was obtained.
        Optionally provide a document frequency, otherwise the default value is 1.
        '''
        # if the term is in the index already, 
        if term in self.term_dictionary.keys():
            if doc_ID not in self.postings[self.term_dictionary[term].postings - 1]:
                # add the docID to the postings list if it is unique and increment the doc_freq
                self.postings[self.term_dictionary[term].postings - 1] += f",{doc_ID}"
                self.term_dictionary[term].doc_freq += 1
        else: # otherwise, create a new entry in the index
            self.term_dictionary[term] = DictionaryEntry(postings_address=len(self.postings) + 1)
            self.postings.append(doc_ID)
            
    def termwise_sort(self):
        '''
        sort the index by term in alphabetical order
        '''
        self.term_dictionary = dict(sorted(self.term_dictionary.items()))
        return self


def build_index(in_dir, out_dict, out_postings):
    """
    build index from documents stored in the input directory,
    then output the dictionary file and postings file
    """
    print('indexing...')
    clear_auxiliary_dirs()
    construct_blocks(in_dir)
    final_merged_index = merge_blocks(out_dict, out_postings)
    copy_to_output_postings(final_merged_index, out_postings)
    copy_to_output_dict(final_merged_index, out_dict, out_postings)
    add_doc_id_list(in_dir, out_dict)

def clear_auxiliary_dirs():
    '''
    clears the AUXILIARY_DICT and AUXILIARY_POST directories where auxiliary blocks are stored
    '''
    if os.path.exists(AUXILIARY_DICT):
        for file_name in os.listdir(AUXILIARY_DICT):
            os.remove(f"{AUXILIARY_DICT}/{file_name}")
    if os.path.exists(AUXILIARY_POST):
        for file_name in os.listdir(AUXILIARY_POST):
            os.remove(f"{AUXILIARY_POST}/{file_name}")

def construct_blocks(in_dir):
    '''
    Parse all files in in_dir and create a partitioned index in "blocks"
    '''
    stemmer = nltk.stem.porter.PorterStemmer()      # one persistent stemmer object
    file_list = os.listdir(in_dir)                  # obtain list of document names
    file_list.sort(key=lambda f: int(f))            # sort document names in algebraically increasing order
    
    if not os.path.exists(AUXILIARY_DICT): os.makedirs(AUXILIARY_DICT+'/')
    if not os.path.exists(AUXILIARY_POST): os.makedirs(AUXILIARY_POST+'/')
    
    index = Index()                                 # initialize the index object
    block_index = 0                                 # track block numbers for filenaming
    num_files = 0                                   # counter for running test mode, which parses only 100 files
    if VERBOSE: print(f"starting new block ({block_index})")
    for file in file_list:
        if TEST_MODE and num_files == 100:
            break
        num_files += 1
        with open(f"{in_dir}/{file}", "r") as doc:
            for line in doc:
                for token in tokenize(stemmer, line):                           # tokenize and stem the files
                    index.insert(term=token, doc_ID=file)
                if len(index) > MAX_BLOCK_SIZE:                                 # if the index size exceeds the allocated block size,
                    if VERBOSE: print(f"starting new block ({block_index})")     # write to disk to free memory and start the next block
                    write_block(index, block_index)
                    block_index += 1
                    index = Index()
                
    # flush remaining data to disk
    if len(index) > 0:                                                          # once all files are parsed, write whatever is left
        if VERBOSE: print(f"writing last block ({block_index})")                 # in the index to disk
        write_block(index, block_index)                 
        
def tokenize(stemmer, line):
    '''
    Given a stemmer object and a list of words,
    return a list of these words tokenized and stemmed using
    nltk.word_tokenize and the porter stemming algorithm
    '''
    tokenized = nltk.word_tokenize(line)
    # UNCOMMENT THIS IF PUNCTUATION REMOVAL IS DESIRED
    # pattern = r'[!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]+'
    # tokenized = filter(lambda x: not re.fullmatch(pattern, x), tokenized)
    return [stemmer.stem(token) for token in tokenized]

def write_block(index: Index, block_index):
    '''
    given an index object and its partition index, save it to disk
    '''
    # sort dictionary by terms before storing
    dictionary = index.termwise_sort().term_dictionary
    postings = index.postings
    with open(f'{AUXILIARY_DICT}/d{block_index}.txt', "w") as dictionary_file:
        for term, entry in dictionary.items():
            dictionary_file.write(format_dict_entry(term, entry.doc_freq, entry.postings))
    with open(f'{AUXILIARY_POST}/p{block_index}.txt', "w") as postings_file:
        for posting_list in postings:
            postings_file.write(f"{posting_list}\n")
            
def format_dict_entry(term, doc_freq, postings):
    '''
    return a formmated string representation of a given dictionary entry
    '''
    return f"{term} {doc_freq} {postings}\n"

'''
Driver method for merging intermediate blocks.
Treats AUXILIARY_DICT directory as a FIFO queue as part of the merge algorithm on
all index partitions created in construct_blocks().
Returns index of final merged block
'''
def merge_blocks(out_dict, out_postings):
    blocks = deque(os.listdir(AUXILIARY_DICT))
    next_block_number = len(blocks)
    while len(os.listdir(AUXILIARY_DICT)) > 1:
        # add new merged block to queue for subsequent merging
        blocks.append(block_merge(blocks[0], blocks[1], next_block_number))
        if VERBOSE: print(f"removing {blocks[0]} and {blocks[1]} and their postings files")
        os.remove(f"{AUXILIARY_POST}/p{blocks[0][1:]}")
        os.remove(f"{AUXILIARY_DICT}/{blocks.popleft()}")
        os.remove(f"{AUXILIARY_POST}/p{blocks[0][1:]}")
        os.remove(f"{AUXILIARY_DICT}/{blocks.popleft()}")
        next_block_number += 1
    if VERBOSE: print(f"Index of final block: {next_block_number-1}")
    return next_block_number - 1
    
'''
Consumes two blocks to merge, returns name of merged block
Because it is a 2-way merge, each block is processed one half 
at a time. This is based off the assumption that we can only
afford one total block's worth of memory.  
Realistically, since we are also chunking the output disk-writes,
we would need to chunk the inputs in thirds to accomodate for 
the memory consumption of the output buffer. 
'''
def block_merge(block_A, block_B, next_block_number):
    if VERBOSE: print(f"Merging {block_A} and {block_B} to make block_{next_block_number}.")
    with open(f"{AUXILIARY_DICT}/{block_A}", "r") as d_A, open(f"d/{block_B}", "r") as d_B:
            
        dict_output_buffer = []        
        post_output_buffer = []
        
        def write_to_buffer(term, df, index, postings):
            '''
            To minimize disk-writes, we also chunk the output. This function
            adds data to the buffer to be written to the dictionary and postings files.
            '''
            nonlocal dict_output_buffer
            nonlocal post_output_buffer
            dict_output_buffer.append(format_dict_entry(term, df, index))
            post_output_buffer.append(postings)
        
        def flush_buffered_chunk():
            '''
            Flush output chunk to disk
            '''
            nonlocal dict_output_buffer
            nonlocal post_output_buffer
            with open(f"{AUXILIARY_DICT}/d{(next_block_number)}.txt", "a") as merged_dict:
                merged_dict.writelines(dict_output_buffer)
                dict_output_buffer = []
            with open(f"{AUXILIARY_POST}/p{next_block_number}.txt", "a") as merged_postings:
                merged_postings.writelines(post_output_buffer)
                post_output_buffer = []
        
        # Must re-index the postings pointers 
        new_entry_index = 1

        # use FIFO queues to maintain alphabetical order
        chunk_A = deque(islice(d_A, CHUNK_SIZE))
        chunk_B = deque(islice(d_B, CHUNK_SIZE))
        # if either file empties, stop doing comparisons for merge
        while chunk_A and chunk_B:
            # get the next entries
            term_A, df_A, postings_A = chunk_A[0].strip().split(" ")
            term_B, df_B, postings_B = chunk_B[0].strip().split(" ")
            # if next term in chunk A alphabetically precedes 
            # the next term in chunk B, add it to the buffer
            if term_A < term_B:
                postings = linecache.getline(f"{AUXILIARY_POST}/p{block_A[1:]}", int(postings_A))
                write_to_buffer(term_A, df_A, new_entry_index, postings)
                chunk_A.popleft()
            # if term A and term B are the same, merge their postings lists,
            # add their document frequencies and add the merged entry to the buffer
            elif term_A == term_B:
                df = str(int(df_A) + int(df_B))
                # sorted postings list are read from disk and then converted into integer lists for merging with heapq.merge()
                postings_list_A = [int(x) for x in linecache.getline(f"{AUXILIARY_POST}/p{block_A[1:]}", int(postings_A)).strip().split(",")]
                postings_list_B = [int(x) for x in linecache.getline(f"{AUXILIARY_POST}/p{block_B[1:]}", int(postings_B)).strip().split(",")]
                postings_list_merged = ','.join([str(x) for x in merge(postings_list_A, postings_list_B)])
                write_to_buffer(term_A, df, new_entry_index, f'{postings_list_merged}\n')
                chunk_A.popleft()
                chunk_B.popleft()
            # if term B precedes term A, add it to the buffer
            elif term_A > term_B:
                postings = linecache.getline(f"{AUXILIARY_POST}/p{block_B[1:]}", int(postings_B))
                write_to_buffer(term_B, df_B, new_entry_index, postings)
                chunk_B.popleft()
            new_entry_index+=1
            # if either chunk runs out, load the next chunk for that file and flush the buffer
            if not chunk_A:
                chunk_A = deque(islice(d_A, CHUNK_SIZE))
                flush_buffered_chunk()
            if not chunk_B:
                chunk_B = deque(islice(d_B, CHUNK_SIZE))
                flush_buffered_chunk()
            
        # once either chunk is empty, write the remainder
        while chunk_A:
            term_A, df_A, postings_A = chunk_A.popleft().strip().split(" ")
            postings = linecache.getline(f"{AUXILIARY_POST}/p{block_A[1:]}", int(postings_A))
            write_to_buffer(term_A, df_A, new_entry_index, postings)
            new_entry_index+=1
            # if chunk_A runs out, try to load another chunk
            if not chunk_A: chunk_A = deque(islice(d_A, CHUNK_SIZE))
        while chunk_B:
            term_B, df_B, postings_B = chunk_B.popleft().strip().split(" ")
            postings = linecache.getline(f"{AUXILIARY_POST}/p{block_B[1:]}", int(postings_B))
            write_to_buffer(term_B, df_B, new_entry_index, postings)
            new_entry_index+=1
            # if chunk_B runs out, try to load another chunk
            if not chunk_B: chunk_B = deque(islice(d_B, CHUNK_SIZE))
        flush_buffered_chunk()
                
    return f"d{(next_block_number)}.txt"

def add_doc_id_list(in_dir, out_dict):
    '''
    Add a list of all document IDs to the dictionary file to
    facilitate NOT queries in search.py
    '''
    file_list = os.listdir(in_dir)
    file_list.sort(key=lambda f: int(f))
    with open(out_dict, "r+") as dictionary_file:
        dictionary_content = dictionary_file.read()
        dictionary_file.seek(0,0)
        dictionary_file.write(f'{" ".join(file_list)}\n{dictionary_content}')
    
def copy_to_output_dict(aux_file_index, out_dict, postings_file):
    '''
    Copies term_dict_file to out_dict, converting the line-number pointers into byte-offset pointers
    '''
    with open(f"{AUXILIARY_DICT}/d{aux_file_index}.txt", "r") as term_dict, \
        open(postings_file, "r") as postings, \
        open(out_dict, "w") as out_dict_file:
        total_offset = 0
        for dict_entry, posting_list in zip(term_dict, postings):
            term, doc_freq, line_num = dict_entry.strip().split(" ")
            postings_list_len = len(posting_list.encode('utf-8'))
            out_dict_file.write(f"{term} {doc_freq} {total_offset} {postings_list_len}\n")
            total_offset += postings_list_len
            
def copy_to_output_postings(aux_file_index, out_postings):
    '''
    Add skip pointers to the postings file and store it in the appropriate output location.
    Skip intervals are the square root of the length of the postings list.
    Based on this calculation, postings lists that contain at least 16 documents 
    can benefit from skip pointers. Anything less would result in equal or more
    work than simply parsing the list linearly. 
    '''
    with open(f"{AUXILIARY_POST}/p{aux_file_index}.txt", "r") as postings_file, open(out_postings, "w") as skips:
        for line in postings_file:
            postings_list = line.strip().split(',')
            skipped = ""
            skip_interval = 0
            if len(postings_list) >= 16:
                skip_interval = floor(sqrt(len(postings_list)))
                
            for i, docID in enumerate(postings_list):
                skip_to = min(i + skip_interval, len(postings_list) - 1)
                skipped += (f'({docID},{skip_to}) ')
            
            skips.write(skipped + '\n')

            
input_directory = "/user/e/e1025440/nltk_data/corpora/reuters/training/"
output_file_dictionary = 'dictionary.txt'
output_file_postings = 'postings.txt'

try:
    opts, args = getopt.getopt(sys.argv[1:], 'i:d:p:t:v')
except getopt.GetoptError:
    usage()
    sys.exit(2)

for o, a in opts:
    if o == '-i': # input directory
        input_directory = a
    elif o == '-d': # dictionary file
        output_file_dictionary = a
    elif o == '-p': # postings file
        output_file_postings = a
    elif o == '-t': # test mode
        TEST_MODE = True        
    elif o == '-v': # verbose mode
        VERBOSE = True
    else:
        assert False, "unhandled option"

if input_directory == None or output_file_postings == None or output_file_dictionary == None:
    usage()
    sys.exit(2)

if TEST_MODE:
    MAX_BLOCK_SIZE = 350
else:
    MAX_BLOCK_SIZE = 3500
    
CHUNK_SIZE = MAX_BLOCK_SIZE//2

if __name__ == "__main__":
    build_index(input_directory, output_file_dictionary, output_file_postings)
