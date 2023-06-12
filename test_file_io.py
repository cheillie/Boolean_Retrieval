from sys import stdout

counter = 0
with open('dictionary.txt', 'r') as file:
    while stink := file.readline():
        breakpoint()
        stdout.write(f"{file.tell()} ")