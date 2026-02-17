# Running on Laptop     ####

Prerequisite:

- Maven 3

- JDK 1.6 or higher

The java main class is:

edu.cs.utexas.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 

# Running:

```WordCount Book-Tiny.txt  OUTPUT.txt```

# Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```mvn clean package```


## How to run 

```
java -jar target/MapReduce-Partitioner-example-0.1-SNAPSHOT-jar-with-dependencies.jar Book-Tiny.txt  output
```

