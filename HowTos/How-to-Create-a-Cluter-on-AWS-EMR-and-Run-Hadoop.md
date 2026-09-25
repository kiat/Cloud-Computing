
# AWS EMR on AWSAcademy Lerner Lab 


## AWSAcademy Lerner Lab  

At the top of these instructions, choose  Start Lab.

* The lab session starts.
* A timer displays at the top of the page and shows the time remaining in the session.
* **Tip:** To refresh the session length at any time, choose  Start Lab again before the timer reaches 0:00.
* Before you continue, wait until the circle icon to the right of the AWS  link in the upper-left corner turns green.

**Note:** This Lab environment will run for 4 hours only and then it terminates. When terminated all Machines and Clusters will terminate but your data on S3 is 
stored safely. 

## Launching an Amazon EMR cluster
In this task, you will launch an EMR cluster with Hive installed.
 
To access the EMR console, in the search box to the right of  Services, search for and choose EMR.

* Go to **EMR on EC2**
* Launch the process to create an EMR cluster.
* Choose Create cluster.
* In the Name and applications section set a Name for this cluster.

For Amazon EMR release, choose **emr-spark-8.0.0** or higher

* Ensure these applications are selected:

Spark 4.0.2 or higher

Clear (deselect) all other selected applications if you do not need them for example Jupyter notebook.

Analysis: The Amazon EMR release that you choose determines the version of Hadoop that will be installed on the cluster. 
Hadoop will install and configure the cluster's internal HDFS as well as the YARN resource scheduler and coordinator to process jobs on the cluster. 

  

## Configure the options - Cluster configuration section
 
* In the Cluster configuration section, set the instance type and number of nodes to use:

*Note: In the console, you might see the main node referred to as the primary node. These instructions will use the term main node.

* For the main node choose **m4.large** from the list.
* Repeat the same process for the Core node type.
* **Important:** Due to lab security settings, if you don't change the instance type as instructed, the EMR cluster creation will fail.
* Verify that under Cluster scaling and provisioning the core Instance size is set to 2.
* Verify that the instance counts are shown as follows in the Summary pane - Core size: 2 instances.

* Analysis: The main node coordinates jobs that will run on the cluster. The main node runs the HDFS NameNode as well as the YARN ResourceManager. The core nodes act as HDFS DataNodes and are where HDFS data is replicated and stored on disk. These nodes also run MapReduce tasks as directed by YARN. Task nodes are an option that can support parallelization and Spot Instance types, but you won't need any for this lab. For more information about HDFS, YARN, MapReduce, and other Hadoop topics, see the Apache Hadoop website.  

## EBS root volume

* Change the Disk Size from 15 to 50GB or 100GB maximum for all nodes. 


## Security configuration and EC2 key pair Info

* Select your own created Key Pair. If you have not created a Key Pair, you can create one, save your own .pem file. 


## Identity and Access Management (IAM) roles - required  

* Select Service role : EMR_DefaultRole
* Instance profile: EMR_EC2_DefaultRole
* (optional) Custom automatic scaling role: EMR_AutoScaling_DefaultRole

Then Click "Create Cluster" and your cluster will be created 

When you are finished with your tasks then you can select this cluster and **Terminate** it. 


# How to run a Hadoop Job on AWS EMR cluster 

* Go to Clusters
* Click on your waiting cluster that is already created and waiting for jobs 
* Click on the Steps and Add Step 
* Custom JAR
* Provide a Name for this Job/Step 
* Provide the S3 location of your jar file, for example:

```
s3://cs378ut/MapReduce-WordCount-example-0.1-SNAPSHOT-jar-with-dependencies.jar
```

* Provide your Arguments. We use the program arguments to define the Input and outputs 

```
s3://cs378ut/Book-Tiny.txt   s3://cs378ut/output
```


Make sure that there is a white space between the two addresses as the first one will be your args[0] string and second one is the args[1] of your application. All of that should be in the same line. 
If you have more arguments like you have an intermediate folder then provide that as well in the order of your application specifications. 

**Important Note** Folders of output or intermediate folder should not exist in your S3 bucket. 

* No Other Change is necessary and you need to Click ADD. Normally it takes about 60 seconds to start. 



# How to SSH to the Master Node 

* Go to EC2 and create a new security group 
* Name it allOpen 
* Add an Inbound Rule that lets all inbound traffic TCP port traffics from 0 to 65535 to go through for your IP address or for all IPs 0.0.0.0/0 
* Add your Master Node to this Security Group 
* Then you can use your .pem file to SSH to the Master Node. 





**Security Note:** It is recommended to open ports only for specific IP ranges and not for all ports because of the security reasons. 
You can open only your own IP address or for a range of IPs. 


For example, you can add a inbound rule the allows all IP ranges of UT Austin Campus. 

* 128.62.0.0/16 — 65,536 IP addresses
* 128.83.0.0/16 — 65,536 IP addresses
* 129.116.0.0/16 — 65,536 IP addresses
* 146.6.0.0/16 — 65,536 IP addresses
* 198.213.192.0/18 — 16,384 IP addresses
* 206.76.64.0/18 — 16,384 IP addresses
* 198.214.80.0/20 — 4,096 IP addresses

Additional Associated Ranges:
* 129.114.0.0/17 — 32,768 IP addresses
* 198.213.32.0/19 — 8,192 IP addresses


**For adding a UT Austin ranges, you can add the following:**

128.62.0.0/16,128.83.0.0/16,129.116.0.0/16,146.6.0.0/16,198.213.192.0/18,206.76.64.0/18,198.214.80.0/20,129.114.0.0/17,198.213.32.0/19



* **Using the AWS CLI on Cloud Shell**

To add Port TCP 22 for SSH to a specific security group 
```
aws ec2 authorize-security-group-ingress \
    --group-id <your-security-group-id> \
    --protocol tcp \
    --port 22 \
    --cidr <your-ip-address>/32
```

or for all TCP ports 

```
# 1. Create the new security group (replace vpc-xxxxxx with your VPC ID)
SECURITY_GROUP_ID=$(aws ec2 create-security-group \
    --group-name "allow-all-tcp-group" \
    --description "New group for all TCP traffic" \
    --vpc-id "vpc-xxxxxxxxxxxxxxxxx" \
    --query 'GroupId' \
    --output text)

# 2. Add the all-TCP inbound rule using the generated ID
aws ec2 authorize-security-group-ingress \
    --group-id $SECURITY_GROUP_ID \
    --protocol tcp \
    --port 0-65535 \
    --cidr 0.0.0.0/0

```




# How to See the Hadoop UIs, or Spark History 

* Go to your EMR cluster 
* Go to Applications
* Select On-cluster applications UIs 
* Then you can see all address of your HDFS, YARN and Spark History. 
* These are only available when your Cluster is running. Create your Screenshots when your task is finished and before terminating your 
* Add your Master Node on the EC2 to a security group that has access to the Inbound Ports needed 
* They open the URLs to find the UIs 


# How add your Files to S3

**Method 1 - Slow**
* Go to S3 on AWS 
* Create a Bucket
* Use the Web Browser to add your file by using the Upload Button


**Method 2 - Fast**
* SSH to your Master Node of the Cluster or any Machines on EC2 
* scp your file to the master node or directly download them to the master node using "wget" command 

* Then on the master Node terminal, use aws cli to copy the file to your S3 bucket 

```
aws s3 cp LARGEFILE.bz2   s3://YourBucketName/
```



