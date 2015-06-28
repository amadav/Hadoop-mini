The program calculates and finds the top three sentences in the corpus on basis of probabilities of each of the sentence. The program is divided into three map-reduce jobs each contributing to find the top 3 sentences. Distributed cache has been used for reading the file for the last phase.
Requirements for the program to run correctly.

****IMPORTANT****
--->>> The program has been hardcoded to read the distributed cache files' local path for the corpus file from the location '/opt/corpus.txt'. The file should be named as ‘corpus.txt and in the normal file format with all permissions given.

Please find the EMR report for the codebase test on cluster environment. 

More info about the project at -- https://eee.uci.edu/15s/18415/HW2.pdf
