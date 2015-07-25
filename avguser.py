#!/usr/bin/python

import sys

# Iterator function that returns 1 line at a time
# and strips the whitespace
def read_input(file):
        for line in file:
            yield line.rstrip()
    

def main():
    
    # Read input through iterator and
    # use a for loop to operate on each line
    # input = read_input(sys.argv[1])
    # print input
    #try:
     # filename="example1.txt"
     # fh= open(filename,"r")
    #except:
     # print "File not opened: %s",sys.exc_info()[1]	
    input =  read_input(sys.stdin)
    #input = read_input(fh)
    for line in input:
        
        # Split the line by double colon
        lineSplit = line.split('::')
        
        # Parse only if there are 4 values in a line
        if len(lineSplit) == 4:
            userid=lineSplit[0]
            # Key = MovieID
            movie = lineSplit[1]
            
            # Value = Rating,Date
            rating = lineSplit[2]
            date = lineSplit[3].replace('-','')
            value = movie+','+rating
            
            # Output Key-Value pair
            print '%s\t%s' % (userid, rating)
            
            # Output status to Hadoop Reporter
            # (Necessary for Hadoop Streaming Apps)
            print >> sys.stderr, "report:counter:pyNetflix1,mapper,1"


if __name__ == "__main__":
    main()
