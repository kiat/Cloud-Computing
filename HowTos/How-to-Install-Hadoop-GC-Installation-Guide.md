# Complete Guide: Installing Apache Hadoop 3.5.0 on Google Compute Engine (3-Node Cluster)

This guide covers setting up a 3-node Apache Hadoop cluster (`master`, `worker1`, `worker2`) on Google Compute Engine (GCE) using a single-machine base setup and template/image replication.

---

## Phase 1: Base Machine Setup (Perform on Initial VM)

### Step 1: Create the Hadoop User and Configure Sudoers
Create a dedicated `hadoop` user with no password, grant passwordless `sudo` privileges, and copy your current user's SSH keys so you can log in seamlessly:

```bash
sudo useradd -m -s /bin/bash hadoop
sudo passwd -d hadoop
echo "hadoop ALL=(ALL) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/hadoop

# Copy current user's authorized_keys to the hadoop user
sudo mkdir -p /home/hadoop/.ssh
sudo cp ~/.ssh/authorized_keys /home/hadoop/.ssh/authorized_keys
sudo chown -R hadoop:hadoop /home/hadoop/.ssh
sudo chmod 700 /home/hadoop/.ssh
sudo chmod 600 /home/hadoop/.ssh/authorized_keys
```

### Step 2: Switch to the Hadoop User and Install Java JDK 21

Switch to your new `hadoop` user for all subsequent installations:
```bash
sudo su - hadoop
```

Install OpenJDK 21 and export `JAVA_HOME`:
```bash
sudo apt update
sudo apt install openjdk-21-jdk -y

# Set JAVA_HOME in bash profile
echo "export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc
source ~/.bashrc
```
*Verification:* Run `java -version` and `echo $JAVA_HOME` to confirm paths.

### Step 3: Configure Passwordless SSH for the Hadoop User
Generate an SSH key pair for the `hadoop` user on the master node so it can log into itself and other cluster nodes without a password:
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```
*Verification:* Run `ssh localhost` to confirm it logs in without a password prompt, then type `exit`.

> **NOTE:** This only sets up passwordless SSH for the `hadoop` user *to itself* (`localhost`). Hadoop's `start-dfs.sh`/`start-yarn.sh` scripts also need the `master` node to SSH into `worker1` and `worker2` without a password to launch their daemons remotely. Because this key is generated *before* the worker VMs exist, that part can't be verified yet — see the new **"Verify Passwordless SSH to Workers"** step added in Phase 3, after the worker nodes are created.

### Step 4: Download and Extract Apache Hadoop 3.5.0
Download the latest Apache Hadoop 3.5.0 release, extract it, and place it under `/usr/local/hadoop`:
```bash
wget https://downloads.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
tar -xzvf hadoop-3.5.0.tar.gz
sudo mv hadoop-3.5.0 /usr/local/hadoop
sudo chown -R hadoop:hadoop /usr/local/hadoop
```

Configure environment variables in `~/.bashrc`:
```bash
echo "export HADOOP_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export HADOOP_INSTALL=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_MAPRED_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_COMMON_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_HDFS_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export YARN_HOME=\$HADOOP_HOME" >> ~/.bashrc
echo "export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native" >> ~/.bashrc
echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc

echo  "export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native" >> ~/.bashrc
echo  "export HADOOP_OPTS=\$HADOOP_OPTS-Djava.library.path=$HADOOP_HOME/lib/native" >> ~/.bashrc
echo  "export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH" >> ~/.bashrc



source ~/.bashrc
```

Explicitly configure `JAVA_HOME` inside Hadoop's environment script (`hadoop-env.sh`):
```bash
echo "export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh
```

*Verification:* Run `hadoop version` to verify the installation runs cleanly.

---

## Phase 2: Creating the Cluster Machines (Replication Methods)

Exit the `hadoop` user session (`exit`) back to your administrative user. Choose **one** of the following methods to create your `master`, `worker1`, and `worker2` instances:

### Method 1: Full Replica (Machine Image) - Recommended
Use this approach if you need the new machines to contain the exact same data and software configurations as your current one.
1. **Create a Machine Image:** Captures your current VM state and attached disks. In the Google Cloud Console, navigate to **Compute Engine > Machine images** and click **Create Machine Image**.
2. **Select Source VM:** Give your image a name, select your current virtual machine from the **Source VM instance** dropdown, and click **Create**.
3. **Deploy Clones:** Once the image builds, click its name, click **Create Instance**, name your instances `worker1` and `worker2` (ensuring your base VM acts as `master`), and launch them.

### Method 2: Hardware Replica (Configuration Only)
If you only want the same hardware specs but want a faster shortcut (note: if using a fresh OS template here, you must repeat Phase 1 manually on the new nodes):
1. **Select Existing VM:** Navigate to your VM instances list and click your current machine.
2. **Use Create Similar:** Click the **Create Similar** button at the top of the details page.
3. **Launch New VMs:** Name the instances `worker1` and `worker2` and click **Create**.

### Method 3: Instance Group / Template Based on this VM
You can also leverage GCP's automation:
1. Navigate to **Compute Engine > Instance templates** and select **Create new template from existing VM/instance**.
2. Set the template name, choose your current fully-configured VM, and save.
3. Go to **Instance groups**, click **Create instance group**, choose a managed instance group based on your template, and scale it to 3 instances, assigning proper instance names like `master`, `worker1`, and `worker2`.

---

## Phase 3: Cluster Configuration (Networking & Hadoop Configs)

1. **Configure Internal DNS (`/etc/hosts`):**
   Open `/etc/hosts` on **all 3 machines** (`master`, `worker1`, `worker2`) and add the internal GCE IP addresses mapped to their hostnames:
   ```text
   10.x.x.x master
   10.x.x.y worker1
   10.x.x.z worker2
   ```

2. **Verify Passwordless SSH from Master to Workers:**
   Because `worker1` and `worker2` were created from a clone/image of `master` (Phase 2), they already contain the identical `authorized_keys` file — meaning `master`'s public key is already trusted by both workers. Confirm this now, as the `hadoop` user on `master`:
   ```bash
   ssh worker1 exit
   ssh worker2 exit
   ```
   Each should log in with **no password prompt**. If either one *does* prompt for a password (for example, if you used Method 2 with a fresh OS template), copy the key manually:
   ```bash
   ssh-copy-id hadoop@worker1
   ssh-copy-id hadoop@worker2
   ```
   This step is required — `start-dfs.sh` and `start-yarn.sh` (Phase 4) use SSH under the hood to launch the DataNode/NodeManager daemons on `worker1` and `worker2`, and will silently fail to start them if passwordless SSH isn't working.

3. **Define Workers:**
   On the `master` node, edit `/usr/local/hadoop/etc/hadoop/workers` and list your worker nodes:
   ```text
   worker1
   worker2
   ```
   > **NOTE:** This file is only read by the node that runs `start-dfs.sh`/`start-yarn.sh` (i.e. `master`), so it technically only needs to exist there — but it doesn't hurt to keep it in sync on all nodes via the `scp` step below.

4. **Configure Core, HDFS, MapReduce and YARN Settings:**


On `master`, edit the following files under `/usr/local/hadoop/etc/hadoop/`:

   **`core-site.xml`** — tells every node where the NameNode lives:
   ```xml
   <configuration>
       <property>
           <name>fs.defaultFS</name>
           <value>hdfs://master:9000</value>
       </property>
   </configuration>
   ```

   **`hdfs-site.xml`** — sets replication (2, to match your 2 worker/DataNode setup) and storage directories:
   ```xml
   <configuration>
       <property>
           <name>dfs.replication</name>
           <value>2</value>
       </property>
       <property>
           <name>dfs.namenode.name.dir</name>
           <value>file:///usr/local/hadoop/hdfs/namenode</value>
       </property>
       <property>
           <name>dfs.datanode.data.dir</name>
           <value>file:///usr/local/hadoop/hdfs/datanode</value>
       </property>
   </configuration>
   ```
   Create the matching storage directories. The NameNode directory is only needed on `master`; the DataNode directory is only needed on `worker1` and `worker2`:
   ```bash
   # On master:
   mkdir -p /usr/local/hadoop/hdfs/namenode

   # On worker1 and worker2:
   mkdir -p /usr/local/hadoop/hdfs/datanode
   ```

   **`mapred-site.xml`**  Tells MapReduce to run on YARN instead of the default local mode:
   
   ```xml
   <configuration>
       <property>
           <name>mapreduce.framework.name</name>
           <value>yarn</value>
       </property>
   </configuration>
   ```

   **`yarn-site.xml`** Tells NodeManagers where the ResourceManager is and enables the shuffle service MapReduce needs:
   ```xml
   <configuration>
       <property>
           <name>yarn.resourcemanager.hostname</name>
           <value>master</value>
       </property>
       <property>
           <name>yarn.nodemanager.aux-services</name>
           <value>mapreduce_shuffle</value>
       </property>
   </configuration>
   ```

   Then, use `scp` to mirror these configuration changes over to `worker1` and `worker2` (run from `master`, as the `hadoop` user):
   ```bash
   for node in worker1 worker2; do
     scp /usr/local/hadoop/etc/hadoop/core-site.xml   hadoop@$node:/usr/local/hadoop/etc/hadoop/core-site.xml
     scp /usr/local/hadoop/etc/hadoop/hdfs-site.xml   hadoop@$node:/usr/local/hadoop/etc/hadoop/hdfs-site.xml
     scp /usr/local/hadoop/etc/hadoop/mapred-site.xml hadoop@$node:/usr/local/hadoop/etc/hadoop/mapred-site.xml
     scp /usr/local/hadoop/etc/hadoop/yarn-site.xml   hadoop@$node:/usr/local/hadoop/etc/hadoop/yarn-site.xml
     scp /usr/local/hadoop/etc/hadoop/workers         hadoop@$node:/usr/local/hadoop/etc/hadoop/workers
   done
   ```

5. **Open Firewall Ports for Internal Cluster Communication:**

The "Web UI Ports" firewall section later in this guide only opens ports for **external browser access** to the monitoring dashboards. It does **not** cover the ports the nodes use to talk to *each other* — without these, the DataNodes can't register with the NameNode and the NodeManagers can't register with the ResourceManager, even though each daemon starts up individually without error.

   | Purpose | Port |
   |---|---|
   | NameNode RPC (`fs.defaultFS`) | `9000` |
   | DataNode data transfer | `9866` |
   | DataNode IPC | `9867` |
   | YARN ResourceManager (scheduler/tracker/RM/admin) | `8030`–`8033` |

   If all 3 VMs are in GCP's **default** auto-mode VPC network, this traffic is usually already allowed by the built-in `default-allow-internal` firewall rule (it permits all internal TCP/UDP traffic between instances in that network). If you're using a **custom VPC network**, or want to be explicit, create a rule the same way as the Web UI ports below:
   * **Targets:** your cluster's instance tags (e.g. `hadoop-cluster`)
   * **Source filter:** the VPC's internal IP range (e.g. `10.128.0.0/9`), or your cluster's own tag
   * **Protocols and ports:** TCP `9000, 9866, 9867, 8030-8033`

---

## Phase 4: Format NameNode and Run a Job

1. Log back in as the `hadoop` user on the `master` node:
   ```bash
   su - hadoop
   ```

2. Initialize the Distributed File System (HDFS):
   ```bash
   hdfs namenode -format
   ```
   > Before starting HDFS for the first time. Re-running it later (e.g. after you already have data) generates a new cluster ID and will cause the DataNodes on `worker1`/`worker2` to reject the NameNode and fail to register, since their existing storage still references the old cluster ID.

3. Start HDFS and YARN daemons:
   ```bash
   start-dfs.sh
   start-yarn.sh
   ```
   *Verification:* Run the `jps` command on master to ensure `NameNode`, `ResourceManager`, `SecondaryNameNode`, etc., are running. Run `jps` on `worker1`/`worker2` to confirm `DataNode` and `NodeManager` are running there too, and run `hdfs dfsadmin -report` on master to confirm both workers show up as live nodes (see "Cluster & Node Reports" below).

4. Run the MapReduce Pi estimation test job:
   ```bash
   hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar pi 10 10
   ```

---

## Hadoop 3.5 Web UI Ports & GCE Firewall Configuration

To monitor your Hadoop cluster health, jobs, and file storage via your browser, Hadoop 3.5 exposes several standard Web User Interfaces.


### Default Hadoop 3.5 Web UI Ports:
* **NameNode Web UI (HDFS Status & Files):** Port `9870` (URL: `http://<master-external-ip>:9870`)
* **YARN ResourceManager UI (Cluster Jobs & Applications):** Port `8088` (URL: `http://<master-external-ip>:8088`)
* **MapReduce JobHistory Server:** Port `19888` (URL: `http://<master-external-ip>:19888`)
* **DataNode Web UI:** Port `9864` (URL: `http://<worker-external-ip>:9864`)

### Opening Ports in Google Cloud Firewall:
By default, GCP blocks external access to these ports. To view them from your local machine, create a firewall rule in the Google Cloud Console (**VPC network > Firewall > Create Firewall Rule**):
* **Targets:** All instances in the network (or apply to your cluster tags).
* **Source filter:** `0.0.0.0/0` (or restrict to your specific office/home IP address for enhanced security).
* **Protocols and ports:** Select **TCP** and specify ports: `9870, 8088, 19888, 9864`.

Run the following command to see all listening TCP and UDP ports:

```
sudo ss -tunlp
```


# Log outputs when you run hadoop 

To see less log outputs use the following environment variable before your hadoop command 

```
HADOOP_ROOT_LOGGER=WARN hadoop jar YOURJARFILE ARGUMENTS

```

# Finding Participating Worker Nodes in Hadoop HDFS

To find the participating worker nodes (DataNodes) in your Hadoop HDFS cluster using the terminal, use the **`hdfs dfsadmin -report`** command.

# Verification

**Check Processes (jps):**

* Run the jps command in your terminal.On Master, you should see: NameNode, SecondaryNameNode, and ResourceManager.On Workers, you should see: DataNode and NodeManager.

* Web Interfaces:View HDFS Health Status: Open http://hadoop-master:9870 in your web browser. 

* Under the "Datanodes" tab, you should see 2 live nodes listed.View YARN Cluster Manager: Open http://hadoop-master:8088.


## Cluster & Node Reports

Use these commands to get detailed metrics regarding storage capacity, usage, and node health status:

*   **Detailed Cluster Summary:**
    ```bash
    hdfs dfsadmin -report
    ```
    *Displays a comprehensive summary of the cluster, followed by a detailed, line-by-line report for every single registered worker node.*

*   **Live Nodes Only:**
    ```bash
    hdfs dfsadmin -report -live
    ```
    *Filters out dead or decommissioning nodes to only display currently functioning worker nodes.*

*   **Dead Nodes Only:**
    ```bash
    hdfs dfsadmin -report -dead
    ```
    *Quickly identifies which worker nodes have crashed or lost connection with the NameNode.*


## Worker Node Infrastructure & Topology

Use these commands if you need a clean list of hostnames or want to inspect the cluster's network layout:

*   **Network Rack Topology:**
    ```bash
    hdfs dfsadmin -printTopology
    ```
    *Prints a tree structure of your network topology, showing exactly which worker nodes belong to which rack.*

*   **YARN Compute Nodes:**
    ```bash
    yarn node -list -all
    ```
    *Lists the worker nodes handling the computation (NodeManagers) rather than just storage. Useful if you are running MapReduce or Spark jobs.*
