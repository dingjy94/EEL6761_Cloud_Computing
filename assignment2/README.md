# Program

PageRank.scala: Pure spark pagerank program

getUniversity.scala: Get the top 100 pagerank weight universities from pagerank output

# Output

pureSparkTop100: iteration time of pagerank is 10

Top100University: top 100 univervisity, iteration time also 10

# Run
```
spark/bin/spark-submit --class
GraphxPageRank.GraphPageRank --driver-memory 3g GraphxPageRank.jar /input
```
