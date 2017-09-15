# EEL6761_Cloud_Computing
class assignment

# Assignment 1
Part1's code and output in folder oneWord. The code is just similar to Hadoop's sample WordCount.
Part2's code and output in folder twoWords. My solution is sample, just use two StringTokenizer to read the input file, one StringTokenizer is one word faster than another. Thus, we can easily get double words key by combine these two StringTokenizer.
Part3's code and output in folder wordPattern.The program get three arguments, input, output and word-pattern file's paths instead of only two arguments. Then, add word-pattern into local cache. In mapper class, when setup the map, read the local file and turns it to HashSet. Then, map method is still similier to WordCount, but only write the word contained in HashSet.
