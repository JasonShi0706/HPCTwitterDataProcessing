'''
Cluster and Cloud Computing Assignment 1
HPC Twitter Data Processing
Name: Yangguang Shi
StudentID: 626689
'''

import re
import time
from datetime import datetime
import math
import sys
from mpi4py import MPI
from collections import Counter
import operator
import os

# query = sys.argv[1]
# path_file = sys.argv[2]

query = "car"
path_file = "/Users/sunshine/学习/Melbourne\ Uni/semester4/Assignment1/Yangguang-Shi-626689/miniTwitter.csv"

# The parameter of this function are the start pointer and end pointer of a file
# This function will read the file line by line and search the target words and 
# get the count of a query
def tweet_search(start_byte, end_byte):
	outcome = []
	with open(path_file,'r',encoding='UTF-8') as f:
		f.seek(start_byte)
		count_query = 0
		popular_twitter = []
		popular_topics = []

#This while loop is to read the file from the start pointer to the end pointer
#and get the count of query, the @twitters and the #topics line by line
		while(f.tell() < end_byte and f.tell() < size_file):
			data = f.readline().lower()
			count_query += data.count(query)
#using regular expression to find @twitters and #topics
			target1 = re.findall(r"@[\w]+",data)
			target2 = re.findall(r"#[\w]+",data)
			popular_twitter += target1
			popular_topics += target2			

#count the number of times a given term (word/string) appears
		outcome.append(count_query)
#the Tweeter (the Twitter user) that is mentioned the most
		count_twitters = Counter(popular_twitter)
		outcome.append(count_twitters)
#the topic that is most frequently discussed
		count_topics = Counter(popular_topics)
		outcome.append(count_topics)

	return outcome

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
num_processes = comm.Get_size()

#Get the size of the file
size_file = os.path.getsize(path_file)

#Calculate the size of work assigned for each process
size_assigned_work = int(math.ceil(size_file/float(num_processes)))

if rank == 0:
	start_time = datetime.now()
# This for loop send the start pointer of the file to the corresponding process
	for process in range(1,num_processes):
		comm.send((size_assigned_work*process), dest = process) 

	final_outcome = tweet_search(0, size_assigned_work)
	part_outcome = []

# this for loop is used to collect the results from other processes
# and get the final result
	for process in range(1,num_processes):
		part_outcome = comm.recv(source = process)
		final_outcome[0] = final_outcome[0] + part_outcome[0]
		final_outcome[1] = final_outcome[1] + part_outcome[1]
		final_outcome[2] = final_outcome[2] + part_outcome[2]

# Get the count of the queries
	count_terms = str(final_outcome[0])

# Get the top 10 most mentioned twitters	
	popular_twitters = str(final_outcome[1].most_common(10))

# Get the top 10 hottest topics	
	popular_topics = str(final_outcome[2].most_common(10))

	end_time = datetime.now()
	time_used = str(end_time - start_time)

	print("Total number of query(" + query + "): " + count_terms)
	print("Top 10 most mentioned users: " + popular_twitters)
	print("Top 10 topics: " + popular_topics)
	print("Time used: " + time_used)

else:
#get the start pointer of the file from the master process
	start_byte = comm.recv(source = 0)
#calculate the end pointer of the file
	end_byte = start_byte + size_assigned_work
#using the tweet_search function to get the outcome of this process
	part_outcome = tweet_search(start_byte, end_byte)
#send the result to the master proceess
	comm.send(part_outcome, dest = 0)