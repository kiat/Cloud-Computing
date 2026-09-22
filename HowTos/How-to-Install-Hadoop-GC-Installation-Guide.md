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

### Step 2: Switch to the Hadoop User and Install Java 21
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

2. **Define Workers:**
   On the `master` node, edit `/usr/local/hadoop/etc/hadoop/workers` and list your worker nodes:
   ```text
   worker1
   worker2
   ```

3. **Configure Core and HDFS Settings:**
   Update `core-site.xml` and `hdfs-site.xml` inside `/usr/local/hadoop/etc/hadoop/` to designate `master` as the NameNode/FS default. Then, use `scp` to mirror these configuration changes over to `worker1` and `worker2`.

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

3. Start HDFS and YARN daemons:
   ```bash
   start-dfs.sh
   start-yarn.sh
   ```
   *Verification:* Run the `jps` command on master to ensure `NameNode`, `ResourceManager`, `SecondaryNameNode`, etc., are running.

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


# Log outputs when you run hadoop 

To see less log outputs use the following environment variable before your hadoop command 

```
HADOOP_ROOT_LOGGER=WARN hadoop jar YOURJARFILE ARGUMENTS

```

# Finding Participating Worker Nodes in Hadoop HDFS

To find the participating worker nodes (DataNodes) in your Hadoop HDFS cluster using the terminal, use the **`hdfs dfsadmin -report`** command.

---

# Other Useful Info 

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

---

## 2. Worker Node Infrastructure & Topology

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
