# Please add your team members' names here. 



##  Course Name: CS378 - Cloud Computing 


# How to compile the project

We use Apache Maven to compile and run this project. 

You need to install Apache Maven (https://maven.apache.org/)  on your system. 

Type on the command line: 

```bash
mvn clean compile
```

# How to create a binary runnable package 


```bash
mvn clean compile assembly:single
```


# How to run 

```bash
mvn clean  compile  exec:java@MainClass
```

Run it with other files for example: 

```bash
mvn clean  compile  exec:java@MainClass -Dexec.args="WikipediaPagesOneDocPerLine.txt"
```


