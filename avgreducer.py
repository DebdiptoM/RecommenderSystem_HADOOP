#!/usr/bin/python

from itertools import groupby
from operator import itemgetter
import sys

# Iterator function that returns and separates
# each line by a given separator
def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

# Import the movie titles file from distributed cache,
# and load it into a python dict so each movie year can
# be accessed by a MovieID key
def dict_from_movie_cache(file_name):
    
    # Create a new dictionary and open the movie titles file
    newDict = {}
    f = open(file_name)
    
    # For every line in the file split the line by 2 commas
    # and distribute the values to movieID, year, name.
    # Assign each movie to the dictionary with the key being
    # movieID and the value being the movie year and title.
    for line in f:
        mov, year, name = line.rstrip().split(',',2)
        newDict[mov] = year+','+name
    
    # Close the file and return the dictionary
    f.close()
    return newDict

def main():
    
    # Read input through iterator and load movie titles
    #filename="example1.txt" 
    #fh=open(filename,"r")
    #mapper = read_mapper_output(fh)
    mapper = read_mapper_output(sys.stdin)    
        # For loop to operate on each line
    for line in mapper:
        
        # Groupby is an python iterator tool that is
        # grouping all mapper input by userid
        for userid, group in groupby(mapper, itemgetter(0)):
            try:
                
                # Ratings is the accumulated total of all the ratings per movie
                # Count is the total number of ratings per movie
                # First is the date of the earliest rating
                # Last is the date of the latest rating
                ratings = 0
                count=0
                
                # Iterate through each user rating in a movie grouping
                for userid, value in group:
                    
                    # Accumulate ratings and count
                    rating = value
                    ratings += int(rating)
                    count += 1
                    

                
                # Average together all the ratings per movie
                avg = float(ratings) / count
                
                # Concatenate all the values
                value = avg
                # Output Key-Value pair
                print '%s\t%s' % (userid, value)
                
                # Output status to Hadoop Reporter
                print >> sys.stderr, "report:counter:pyNetflix1,reducer,1"
            
            except ValueError:
                pass

if __name__ == "__main__":
    main()
