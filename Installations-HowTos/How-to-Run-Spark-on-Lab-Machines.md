For anybody struggling to get Spark to work locally, getting it set up on the lab machines is super easy. I recommend using VSCode with the Remote SSH extension.

1. SSH into lab machine of choice

2. Create a folder for the assignment ex. 

```
mkdir InClassAssignment5
cd InClassAssignment5
```


3. Create a virtual environment using 

```
python3 -m venv venv
```


4. Activate your virtual environment using 

```
source venv/bin/activate
```


Your terminal should now show something like "(venv) jdittoe@chocapic:~/ica5$" as your current directory.

 
5. Install pyspark with 



```
pip install pyspark
```



6. Run code with python3 (spark-submit not needed, but can be used instead of python3 and still works), ex. 


```
python3 spark_flight_streaming.py
or
spark-submit spark_flight_streaming.py
```


If you need a second terminal, in VSCode, click on the left icon to split the terminal in the view:


For downloading files you can use wget, and for getting the python files over you can either use SCP or just create a file VSCode and copy/paste the file from your laptop. Whenever coming back to this later, just cd into the directory and repeat step 4, then you can run.
