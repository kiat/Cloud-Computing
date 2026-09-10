
# Socket Server in Java 

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


# How to run the Server 

```bash
mvn clean  compile  exec:java@server -Dexec.args="33333"
```

# How to run the Client

```bash
mvn clean  compile  exec:java@client  -Dexec.args="localhost 33333"
```

Change the localhost with the local ip address of your cloud machine. 


# How to run a Server that can talk to many clients

```bash
mvn clean  compile  exec:java@multiClientServer -Dexec.args="33333"
```






