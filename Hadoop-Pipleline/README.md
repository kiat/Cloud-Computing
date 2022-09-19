
# Project Introduction

This is a Java Maven Project implementing a couple of Map and Reduce jobs using Hadoop MapReduce. 


# Running on Laptop

Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed

- Make


### The java main classes are:


### Input data set 
* taxi-data-sorted-small.csv (for the small dataset)
```
https://storage.googleapis.com/cs378/taxi-data-sorted-small.csv
```

Download https://storage.googleapis.com/cs378/taxi-data-sorted-small.csv

## Running


## Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```	mvn clean package ```

## Create a single JAR File from eclipse
Create a single gar file with eclipse 

*  File export -> export  -> export as binary ->  "Extract generated libraries into generated JAR"
