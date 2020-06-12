#Use Apache Spark to read and write Apache HBase data
These set of scripts helps with HBase and Spark connector docunentation 
https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-using-spark-query-hbase#prepare-sample-data-in-apache-hbase

###Prerequisites
<!-- @import "[TOC]" {cmd="toc" depthFrom=1 depthTo=6 orderedList=false} -->

<!-- code_chunk_output -->

- [Use Apache Spark to read and write Apache HBase data](#use-apache-spark-to-read-and-write-apache-hbase-data)
    - [Prerequisites](#prerequisites)
    - [What does the script do?](#what-does-the-script-do)

<!-- /code_chunk_output -->
1. Two separate secure HDInsight clusters deployed in the same virtual network. One HBase, and one Spark with at least Spark 2.1 (HDInsight 3.6) installed. 
2. Optional but recommended step: Spark cluster storage added as HBase cluster secondary storage, see Add additional storage accounts to HDInsight. Before proceeding, make sure your cluster is running without errors on Ambari after secondary storage is added.
###What does the script do?
If Spark storage is added to HBase as secondary storage:
1.	Copy hbase-site.xml file onto the spark storage account (through hdfs)
2.	Copy HBase IP mapping onto the spark storage account (through hdfs)
3.	On Spark cluster head nodes:
a.	Move hbase-site.xml file to the sparks configuration folder (etc/spark2/conf)
b.	Add HBase IP mapping to /etc/hosts file

If Spark storage is NOT added to HBase as secondary storage:
1.	Copy hbase-site.xml file onto the spark head nodes (through scp)
2.	Copy HBase IP mapping onto the spark head nodes (through scp)
3.	On Spark cluster head nodes:
a.	Move hbase-site.xml file to the sparks configuration folder (etc/spark2/conf)
b.	Add HBase IP mapping to /etc/hosts file

Scaling Scenario for ESP Cluster:
1.	On HBase cluster scale up: one and only one of the newly added nodes will make communication with Spark head nodes and update Spark /etc/hosts file with the new HBase IP mapping.
2.	On HBase cluster scale down: No communication/changes (thus no change in /etc/hosts) will be reported to Spark head nodes. This means that your Spark cluster’s /etc/hosts file will contain old entries with IP mappings from previous scale up. However, this should not affect the connection between HBase and Spark. 
3.	Currently the script does not support scaling of Spark Cluster, if you wish to scale your spark cluster, please ensure you manually modify the /etc/hosts file on Spark cluster to include HBase /etc/hosts entries
How to run script action?

Use Script Action on your HBase cluster to apply the changes with the following considerations:



| Property                         | Value                             |
|----------------------------------|-----------------------------------|
| Bash script URI                  | url of the location of the script |
| Node type(s)                     | Region                            |
| Parameters                       | -u SPARK_SSH_USER (required)      |
| -n SPARK_CLUSTER_NAME (required) |
| - s SPARK_CLUSTER_SUFFIX         |
| -a SPARK_STORAGE_ACCOUNT         |
| -c SPARK_STORAGE_CONTAINER       |
| -k SPARK_SSH_KEY_URL             |
|    or (at least one is required) |
| -p SPARK_SSH_PASSWORD            |

Example: 
-u sshuser -n mysparkcluster -a sparkstorageacc -c sparkstoragecontainer -p mySparkPassw0rd

###Run Spark Shell referencing the Spark HBase Connector
After completion of above step, you should be able to run spark shell referencing to the appropriate version of Spark HBase Connector provided by Hortonworks.

Please reference to OSS Spark Hive Connector Core Repository for the most recent corresponding SHC Core version for your cluster version scenario. Below table are examples of the command HDInsight team uses for testing:

1.	From your open SSH session to the Spark cluster, enter one of the commands below (or corresponding command with proper SHC version) to start a spark shell:

| Spark Version | HDI HBase Version   | SHC Version      | Command                                                                                                                               |
|---------------|---------------------|------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| 2.4           | HDI 4.0 (HBase 2.0) | 1.1.1-2.1-s_2.11 | spark-shell --packages com.hortonworks.shc:shc-core:1.1.0.3.1.2.2-1 --repositories http://repo.hortonworks.com/content/groups/public/ |
| 2.1           | HDI 3.6 (HBase 1.1) | 1.1.0.3.1.2.2-1  | spark-shell --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories https://repo.hortonworks.com/content/groups/public/   |



2. Keep this Spark Shell instance open and continue to the next step.
https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-using-spark-query-hbase#define-a-catalog-and-query
