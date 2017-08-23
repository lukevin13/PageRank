# PageRank
This repository contains an Java project that implements the Page Ranking algorithm with map reduce. This is a component of the final project of Spring 2016's **Internet and Web Systems Course** at the University of Pennsylvania.

## Implementation
#### PageRank
The PageRank of a page is determined using the random surfer model. This model simulates a bored web surfer who is randomly clicking on links on any given web page within the corpus. The determined pagerank of a page A is representative of the probability that page A will be reached by this random surfer. The formula used for each pagerank mapreduce iteration is as follows:

![Page Rank Formula](https://wikimedia.org/api/rest_v1/media/math/render/svg/9f853c33de82a94b16ff0ea7e7a7346620c0ea04)

where d is the damping coefficient 0.85; PR(pj) is the pagerank of page pj; L(pj) is the number of outgoing links on page pj; N is the number of pages in the index.

#### Corpus Retrieval & Conversion:
The PageRank component must retrieve the existing corpus. This is done through a hashing method that will divide up the the files in the corpus for each worker in the cluster to download. No two workers will retrieve document unless that document appears twice in the corpus. After retrieving the documents in the corpus, each worker must convert the raw html content of each document into the input format for the mappers.

#### Map:
This mapper takes the rank found in the key string, k,  and divides it by the number of links in the value string, v. This becomes the weight of each outgoing link, w. For every link l in the value string, the mapper will emit(l, w). It will then emit(k, v) so that the reducer can create an output which can be used as the input for the next mapreduce iteration.

#### Shuffle & Sort:
After all mappers have completed their map jobs, a shuffle & sort phase will upload the output files of the mappers to the S3 and then download the files to the correct reducer machine determined by the hash function in the map phase.

#### Reduce:
The reducer will sum all the values found in a given list of values (with the exception of the value entry containing the list of outgoing links). It will then the carry out the specified the pagerank formula and emit the page concatenated with the pagerank and the outgoing links of that page.

#### Database Update & Queries:
At the end of each mapreduce iteration, a Berkeley database with the pagerank values of each page in the corpus is updated with their new pagerank by reading the keys of each reducer output. This updated database is then uploaded to S3. Queries to the pagerank servlet simply check and return the database for the pagerank of the received page query.

## Execution
* Start a master servlet on a machine by running the *runmaster.sh* script
* Start a worker servlet on a machine by running the *runworker.sh* script

**Note:** This application will **not** work anymore given that the AWS account used for this project has long been deactivated. However, feel free to look at the (poorly commented- apologies in advance) source code.

## Some Lessons Learned Here
* Map reduce is pretty darn amazing
* Really hurt myself by not writing better documentation and comments while I was working on this project - will definitely avoid making this mistake again in future projects
