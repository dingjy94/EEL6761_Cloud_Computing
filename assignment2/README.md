# Program

PageRank.scala: Pure spark pagerank program

getUniversity.scala: Get the top 100 pagerank weight universities from pagerank output

GraphxPageRankPre.scala: Pre processing for GraphX pageranl

GraphXPRMain.scala: GraphX pagerank program

# Output

pureSparkTop100: iteration time of pagerank is 10

Top100University: top 100 univervisity, iteration time also 10

# Run
```
spark/bin/spark-submit --class
MainClass --driver-memory 3g(must larger than 2g) jar args
```
# master URL
http://ec2-34-227-110-128.compute-1.amazonaws.com/

wex: /input
