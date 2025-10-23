
# Step-1 - Setup Java JDK

Install java JDK on your machine 


java -version

java version "1.8.0_211"
Java(TM) SE Runtime Environment (build 1.8.0_211-b12)
Java HotSpot(TM) 64-Bit Server VM (build 25.211-b12, mixed mode)


and setup 

JAVA_HOME variable to the main folder where you have installed your java 

```
export JAVA_HOME=`/usr/libexec/java_home -v 1.8
```
 * *Note:* It is important to install Java JDK (Java Development Kit). Java runtime (JRE) is not enought to run spark.

You can also use brew to install JDK. Install openJDK if you have brew on your MacOS. 

```
brew install openjdk@21
```



# Step-2 - Download Apache Spark 


Go to the Spark website and download spark binary package 
* *NOTE:* Check the Spark website for the latest download link 

http://spark.apache.org/downloads.html


Direct link is for example following 

[http://mirror.metrocast.net/apache/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz](https://www.apache.org/dyn/closer.lua/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz)

You can run the following command to download: 

```wget https://www.apache.org/dyn/closer.lua/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz```


Uncompress it 

```tar xvfz spark-4.0.1-bin-hadoop3.tgz```

I have my spark on the following folder 

```/Users/YOUR-ACCOUNT/spark-4.0.1-bin-hadoop3.tgz```

# Step-3 -  Setup Enviroment Variables. 

Edit the bash_profile file and set some Environment variables 

```nano .bash_profile```

**Note:** For maxOS Catalina user, you need to edit the zshrc file and set the below enviroment variables to the end of your file 

Also, if your zsh shell could not find the jupyter notebook command, here is the link to solve this problem

https://medium.com/@sumitmenon/how-to-get-anaconda-to-work-with-oh-my-zsh-on-mac-os-x-7c1c7247d896

```nano .zshrc```

```export SPARK_HOME=/Users/YOUR-ACCOUNT/spark-4.0.1-bin-hadoop3```

```export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH```

Set it to the path where you have your python3 
It can be like this 
```export PYSPARK_PYTHON=/Users/YOUR-ACCOUNT/anaconda3/bin/python3```
or 
```export /Library/Frameworks/Python.framework/Versions/3.13/bin/python3```

You can find the path by running 
```which python3```


Add Spark bin and sbin folder to the PATH 

```export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin```



To activate the current variables you can restart your mac or type 

```source .bash_profile``` or
```source .zshrc```

If you want to use the jupyter notebook on your mac set the following two variables as well. 

**Note:** Note that you can only use pyspark ipython shell or jupyter notebooks, one of them only 
You can comment or uncomment the following variables to use one of them  

```export PYSPARK_DRIVER_PYTHON="jupyter"```

```export PYSPARK_DRIVER_PYTHON_OPTS="notebook"```


to activate 
```source .bash_profile```or
```source .zshrc```

# Step-4 - Run Spark 

You can now run the following commands 



```pyspark``` 

pyspark command on the termial will open the Spark Python interactive shell or it will open your Jupyter notebook if you have enable it. 

or 

```spark-submit```

The above commands will run spark on Stand alone mode and in a single process. 

*Note:* If you want ot use spark-submit you need to disable Jupyter Notebook configuratation. Comment out the two enviroment variables  PYSPARK_DRIVER_PYTHON and PYSPARK_DRIVER_PYTHON_OPTS in your .zshrc file. 

Run an example pyspark script 
```spark-submit  your-pyspark-code.py  INPUTFILE OUTPUT-FOLDER```

OUTPUT-FOLDER should not exist before. 



