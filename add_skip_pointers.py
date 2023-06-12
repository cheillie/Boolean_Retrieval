from math import sqrt, floor

def main():
    with open("full_postings.txt", "r") as postings_file, open("full_postings_with_skips.txt", "w") as skips:
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
                    

if __name__ == "__main__":
    main()