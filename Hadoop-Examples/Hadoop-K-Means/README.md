# Running on Laptop     ####

Prerequisite:

- Apache Maven

- Java JDK 1.6 or higher

The java main class is:

edu.cs.utexas.HadoopEx.KMeansDriver 

Input file:  points.txt

Specify your own Output directory like 


# Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
	```mvn clean package```


# Running:

```KMeansDriver input output```


```java -jar target/KMeans-example-0.1-SNAPSHOT-jar-with-dependencies.jar points.txt output ```


