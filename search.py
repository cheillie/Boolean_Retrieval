#!/usr/bin/python3
import re
import nltk
import sys
import getopt
import collections
import os



def usage():
    print("usage: " + sys.argv[0] + " -d dictionary-file -p postings-file -q file-of-queries -o output-file-of-results")

def run_search(dict_file, postings_file, queries_file, results_file):
    """
    using the given dictionary file and postings file,
    perform searching on the given queries file and output the results to a file
    """
    print('running search on the queries...')

    # clear the out_file
    open(results_file, 'w').close()

    stemmer = nltk.stem.porter.PorterStemmer()
    operators_prio = {"AND": 2, "OR": 1, "NOT": 3, "(": 0, ")": 0}
    max_query_len = 1024

    # load dictionary into memory
    dictionary = {}
    with open(dict_file, 'r') as dic_file:
        all_postings_str = dic_file.readline()          # first line in dict_file contains all docIDs
        for line in dic_file:
            term, doc_freq, pointer, num_bytes = line.split(" ")   # omitting doc freq 
            dictionary[term] = [int(pointer), int(num_bytes)]

    # create an all posting tuple list used to NOT operation
    all_postings = all_postings_str.strip().split(" ")
    all_postings = [int(x) for x in all_postings]
    all_postings = list(zip(all_postings, all_postings))

    result_f = open(results_file, "a")

    # Read the queries_file line by line
    with open(queries_file) as file:
        terms = dictionary.keys()
        for line in file:
            # if length of the query exceeds the max query length, no result will be logged
            # or if the query is empty, no result will be logged
            if len(line) > max_query_len or len(line) == 0:
                result_f.write("\n")
                break

            # obtain the query in Reverse Polish notation
            shunting_yard_output = parse_shunting_yard(line, operators_prio, stemmer)

            queue = collections.deque(shunting_yard_output)
            result_stack = []
            while queue:
                answer = []
                # evaluate the transformation from the left
                token = queue.popleft()
                # perform AND operator on the left operand and right operand 
                if token == "AND":
                    if len(result_stack) > 1:
                        right = result_stack.pop()
                        left = result_stack.pop()
                        answer = and_op(left, right)
                    else:
                        result_f.write("INVALID QUERY\n")
                        result_stack = []
                        break
                # perform NOT operator on the right operand 
                elif token == "NOT":
                    if len(result_stack) > 0:
                        right = result_stack.pop()
                        answer = not_op(right, all_postings)
                    else:
                        result_f.write("INVALID QUERY\n")
                        result_stack = []
                        break
                # perform OR operator on the left operand and right operand 
                elif token == "OR":
                    if len(result_stack) > 1:
                        right = result_stack.pop()
                        left = result_stack.pop()
                        answer = or_op(left, right)
                    else:
                        result_f.write("INVALID QUERY\n")
                        result_stack = []
                        break
                # if token is a term, not an operator
                else:
                    if token in terms:
                        # obtain the pointer to the posting
                        pointer = dictionary[token][0]
                        num_bytes = dictionary[token][1]
                        # obtain the corresponding posting pointed by the pointer
                        with open(postings_file) as postings_f:
                            postings_f.seek(pointer, 0)
                            answer = get_posting(postings_f.read(num_bytes))

                result_stack.append(answer)

            # if result stack doesn't have 1 item, that means the query was invalid
            if len(result_stack) == 1:
                # write result to results_file
                result_list = result_stack.pop()
                # if no document found
                if result_list == None:
                    result_f.write("\n")
                else:
                    result_str = str([i[0] for i in result_list])
                    result = re.sub("\[|\]|,", "", result_str)
                    result_f.write(result+"\n")
    result_f.close()
  

def parse_shunting_yard(line, operators_prio, stemmer):
    """
    Stem the input query and parse it based on Reverse Polish notation.
    E.g. bill OR Gates -> bill gates OR
    https://www.youtube.com/watch?v=Jd71l0cHZL0
    """
    result = []
    op_stack = []
    for token in nltk.word_tokenize(line):
        if token not in operators_prio.keys():          # token is a word, move to result
            stemmed_token = stemmer.stem(token)
            result.append(stemmed_token)
        elif token == "(":                              # token is a left bracket, move to result
            op_stack.append(token)
        elif token == ")":                              # token is a right bracket, pop op_stack to result until left bracket is seen
            operator = op_stack.pop()
            while operator != "(":
                result.append(operator)
                operator = op_stack.pop()
        elif token in operators_prio.keys():            # token is an operator, move to result based on precedence
            if op_stack:
                curr_op = op_stack[-1]
                while (op_stack and operators_prio[curr_op] > operators_prio[token]):
                    result.append(op_stack.pop())
                    if op_stack:
                        curr_op = op_stack[-1]
            op_stack.append(token)

    while op_stack:
        result.append(op_stack.pop())                   # pop all operators to result

    return result


def and_op(p1, p2):
    """
    Intersect (AND operation) the left posting list and right posting list with skip pointers,
    and return the a list of tuples (docID, skip pointer) where the docIDs intersect 
    """
    answer = []

    # initiate the posting index
    p1_index = 0
    p2_index = 0

    docID = 0
    skip_pointer = 1
    skip_pointer_gap = 16

    # compare the two postings until one of them reaches the end
    while (p1_index < len(p1)) and (p2_index < len(p2)):
        p1_docID = p1[p1_index][docID]                      # docID of p1
        p2_docID = p2[p2_index][docID]                      # docID of p2
        p1_skip_pointer = p1[p1_index][skip_pointer]        # skip pointer of p1
        p2_skip_pointer = p2[p2_index][skip_pointer]        # skip pointer of p2

        # if the docIDs match, that means we found an answer 
        if p1_docID == p2_docID:
            answer.append(p1[p1_index])
            p1_index += 1
            p2_index += 1

        # if the docID(p1) is less than docID(p2), then check if p1 is skippable
        elif p1_docID < p2_docID:
            # only use skip pointer if the length of the posting is >= than the skip pointer gap
            if len(p1) >= skip_pointer_gap:
                # if we are not at the end and the skip pointer is less than the docID,
                # advance p1 index to be the skip pointer
                while (p1_skip_pointer < len(p1)-1) and (p1[p1_skip_pointer][docID] <= p2_docID):
                    p1_index = p1_skip_pointer
                    p1_skip_pointer = p1[p1_index][skip_pointer]
                else:
                    p1_index += 1
            else:
                p1_index += 1

        # if the docID(p2) is less than docID(p1), then check if p2 is skippable
        else:
            # only use skip pointer if the length of the posting is >= than the skip pointer gap
            if len(p2) >= skip_pointer_gap:
                # if we are not at the end and the skip pointer is less than the docID,
                # advance p2 index to be the skip pointer
                while p2_skip_pointer < len(p2)-1 and (p2[p2_skip_pointer][docID] <= p1_docID):
                    p2_index = p2[p2_index][skip_pointer]
                    p2_skip_pointer = p2[p2_index][skip_pointer]
                else:
                    p2_index += 1
            else:
                p2_index += 1

    return answer
        
        
def or_op(p1, p2):
    """
    Perform OR operation on the left posting list and the right posting list, and return
    a list of tuples (docID, skip pointer) where the docID appears in any of the two postings 
    """
    answer = []

    # initiate the posting index
    p1_index = 0
    p2_index = 0

    docID = 0

    while (p1_index < len(p1)) or (p2_index < len(p2)):
        # If both postings have not reached the end
        if (p1_index < len(p1)) and (p2_index < len(p2)):
            p1_docID = p1[p1_index][docID]      # docID of p1
            p2_docID = p2[p2_index][docID]      # docID of p2

            # if the docIDs match, that means we found an answer 
            if p1_docID == p2_docID:
                answer.append(p1[p1_index])
                p1_index += 1
                p2_index += 1
            # if the docID(p1) is less than docID(p2)
            elif p1_docID < p2_docID:
                answer.append(p1[p1_index])
                p1_index += 1
            # if the docID(p2) is less than docID(p1)
            else:
                answer.append(p2[p2_index])
                p2_index += 1

        # if posting1 reached the end of its posting, add the docIDs in the rest of posting2
        elif p1_index >= len(p1):
            answer.append(p2[p2_index])
            p2_index += 1

        # if posting2 reached the end of its posting, add the docIDs in the rest of posting1
        else:  
            answer.append(p1[p1_index])
            p1_index += 1

    return answer

def not_op(right_posting, all_postings):
    """
    Perform NOT operation on the right posting and return a list of tuples (docID, skip pointer)
    where the docID does not appear in the right posting
    
    """
    answer = []
    p1 = get_posting(right_posting)
    p1_index = 0
    docID = 0
    
    # if p1 is empty, that means every posting is the result
    if not p1:
        return all_postings
    else:
        for posting in all_postings:
            if posting[0] != p1[p1_index][docID]:
                answer.append(posting)
            elif p1_index + 1 < len(p1):
                p1_index += 1
        return answer


def get_posting(postings):
    """
    Parse the postings into a list of tuples. Where the first item in the tuple is the docID, 
    and the second item in the tuple is the skip pointer
    e.g. (2,3) (3,10) (4,5) -> [(2, 3), (3, 10), (4, 5)]
    """
    if postings and type(postings) != list:
        p = []
        postings = "".join(postings.rstrip())
        postings = postings.split(" ")
        for posting in postings:
            p.append(eval(posting))
        return p
    else:
        return postings
            

file_of_output = "/home/e/e1100368/CS3245/CS3245/HW2/output.txt"
file_of_queries = "/home/e/e1100368/CS3245/CS3245/HW2/queries.txt"
postings_file = "/home/e/e1100368/CS3245/CS3245/HW2/postings.txt"
dictionary_file = "/home/e/e1100368/CS3245/CS3245/HW2/dictionary.txt"

try:
    opts, args = getopt.getopt(sys.argv[1:], 'd:p:q:o:')
except getopt.GetoptError:
    usage()
    sys.exit(2)

for o, a in opts:
    if o == '-d':
        dictionary_file  = a
    elif o == '-p':
        postings_file = a
    elif o == '-q':
        file_of_queries = a
    elif o == '-o':
        file_of_output = a
    else:
        assert False, "unhandled option"

if dictionary_file == None or postings_file == None or file_of_queries == None or file_of_output == None :
    usage()
    sys.exit(2)

run_search(dictionary_file, postings_file, file_of_queries, file_of_output)
