# Map Reduce Program using Apache Spark

### The application collects thousands of tweets and searches keywords in all of them through Map-Reduce program. This is just a small sample program to better grasp the implementation of map-reduce.

## Why do we need Map-Reduce?

We use MapReduce to write scalable applications that can do parallel processing to process a large amount of data on a large cluster of commodity hardware servers.

## Steps to run
* Go to the root diarectory.
* Run ```mvn clean install```
* Add the folder "inputFiles" inside "target" folder before running the jar.
* Go to target folder and run command ```spark-submit MapReducer.jar```

![Activity Diagram](/images/command.jpg "This is a sample image.")

## Output
![Screen Output.](/images/output.jpg "Output")

## Tools and Languages used

* Java
* Maven
* Apache Spark
