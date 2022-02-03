
##############################
### Running on Laptop     ####
##############################


Prerequisite:

- Maven 3

- JDK 1.6 or higher

- (If working with eclipse) Eclipse with m2eclipse plugin installed


The java main class is:

edu.bu.metcs.HadoopEx.WordCount 

Input file:  Book-Tiny.txt  

Specify your own Output directory like 



running:

	WordCount Book-Tiny.txt  OUTPUT.txt

###########################################
##### Create a JAR File from eclipse ######
###########################################


Create a single gar file with eclipse 

	File export -> export  -> export as binary -> 

	"Extract generated libraries into generated JAR"


###########################################
##### Create a JAR Using Maven       ######
###########################################


# Install maven - compile 
To compile the project and create a single jar file with all dependencies: 
	
	mvn clean compile assembly:single




##############################
## Tutorial Link for Maven ###
##############################

Hadoop: Setup Maven project for MapReduce in 5mn
https://hadoopi.wordpress.com/2013/05/25/setup-maven-project-for-hadoop-in-5mn/







