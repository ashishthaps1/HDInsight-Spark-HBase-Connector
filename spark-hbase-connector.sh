#!/bin/sh

# Parameters:
# 1. ssh username - REQUIRED
# 2. spark cluster name - REQUIRED
# 3. spark cluster name 
# 4. spark storage account
# 5. spark storage container
# 6. ssh key - REQUIRED IF USING KEY
# 7. ssh password - REQUIRED IF USING PASSWORD

# Example: 
# SPARK_SSH_USER="sshuser"
# SPARK_CLUSTER_NAME="sparkshc"
# SPARK_CLUSTER_SUFFIX="hdinsight-stable.azure-test.net"
# SPARK_STORAGE_CONTAINER="sparkcontainer"
# SPARK_STORAGE_ACCOUNT="sparkshchdistorage"
# SSH_KEY_URL=https://sparkshchdistorage.blob.core.windows.net/sparkcontainer/hdiprivate.key
# SSH_PASSWORD="TestPassW0rd"

trap Remove_temporary_files_on_HBase_WN 0 1 2 3 15 

# redirect script output to system logger with file basename
exec 1> >(logger -s -t $(basename $0)) 2>&1

Help()
{
   echo ""
   echo "Example Usage: $0 -u spark_ssh_username -s spark_ssh_endpoint -p spark_ssh_password"
   echo -e "\t-u spark_ssh_username - Required"
   echo -e "\t-n spark_cluster_name - Required"
   echo -e "\t-s spark_cluster_suffix - Default is azurehdinsight.net"
   echo -e "\t-a spark_storage_account"
   echo -e "\t-c spark_storage_container"
   echo -e "\t-p spark_ssh_password - Required if password authentication"
   echo -e "\t-k spark_ssh_privatekey_url - Required if private key authentication"
   exit 1 # Exit script after printing help
}

Get_HBase_Host_Mappings()
{
    #  take ip mapping in /etc/hosts file and copy to a temporary file
    sudo rm -f /tmp/hbase-etc-hosts
    sed -n '/ip6-allhosts/,$p' /etc/hosts | sed '1d' >> /tmp/hbase-etc-hosts

    # get hostname of the cluster
    sudo rm -f /tmp/hbase-hostname
    hostname > /tmp/hbase-hostname
    sed -i 's/^[^-]*-/-/' /tmp/hbase-hostname
}

# remove key & lock & tmp files if exists
Remove_temporary_files_on_HBase_WN()
{
    sudo rm -f /tmp/sparkprivate.key
    sudo rm -f /tmp/lock
    sudo rm -f /tmp/hbase-hostname
    sudo rm -f /tmp/hbase-etc-hosts
    sudo hdfs dfs -rm -f /tmp/lock
}


# use this method when user do not have spark storage as their secondary storage account
# Have not been thoroughly tested, use with caution, highly recommend user to add spark as their secondary storage acc.
Copy_by_scp()
{
    # remove testkey.txt previously scp-ed if using private key, move hbase-site to proper folder and change uid:gid
    SCPhbasesiteCommandsToRun="rm -f testkey.txt && 
        sudo mv /tmp/hbase-site.xml /etc/spark2/conf/hbase-site.xml && 
        sudo chown spark:spark /etc/spark2/conf/hbase-site.xml"
    
    # copy etc/hosts files from hbase to spark (remove old hbase ip mapping and add new hbase ip mapping)
    # remove any temporary files created
    SCPetchostfileCommandsToRun="sudo cp /etc/hosts /tmp/etc-hosts-copy &&
        sudo cat /tmp/hbase-hostname | xargs -i sudo sed -i '/{}/d' /tmp/etc-hosts-copy && 
        sudo bash -c 'cat /tmp/hbase-etc-hosts >> /tmp/etc-hosts-copy' && 
        sudo cp /tmp/etc-hosts-copy /etc/hosts && 
        sudo rm -f /tmp/hbase-hostname && sudo rm -f /tmp/etc-hosts-copy && sudo rm -f /tmp/hbase-etc-hosts"

    if [ -n "${SSH_KEY_URL}" ]; then
        echo "SHC Connector: Copying hbase-site.xml, HBase hostname, HBase IP mapping to Spark cluster with scp key... "
        sudo scp -i /tmp/sparkprivate.key /etc/hbase/conf/hbase-site.xml /tmp/hbase-hostname /tmp/hbase-etc-hosts $SPARK_SSH:/tmp
        sudo scp -P 23 -i /tmp/sparkprivate.key /etc/hbase/conf/hbase-site.xml /tmp/hbase-hostname /tmp/hbase-etc-hosts $SPARK_SSH:/tmp

        echo "SHC Connector: Copying hbase-site.xml to Spark configuration folder... "   
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key $SPARK_SSH -t $SCPhbasesiteCommandsToRun
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key -p 23 $SPARK_SSH -t $SCPhbasesiteCommandsToRun
        echo "SHC Connector: hbase-site.xml copy completed."

        echo "SHC Connector: Modifying /etc/hosts with HBase ip mapping with private key... "
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key $SPARK_SSH -t $SCPetchostfileCommandsToRun
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key -p 23 $SPARK_SSH -t $SCPetchostfileCommandsToRun
        echo "SHC Connector: /etc/hosts modification completed."

    else
        echo "SHC Connector: Copying hbase-site.xml, HBase IP mapping, HBase hostname to spark cluster with scp password... "
        sudo sshpass -p $SSH_PASSWORD scp -o StrictHostKeyChecking=no /etc/hbase/conf/hbase-site.xml /tmp/hbase-etc-hosts /tmp/hbase-hostname $SPARK_SSH:/tmp
        sudo sshpass -p $SSH_PASSWORD scp -o StrictHostKeyChecking=no -P 23 /etc/hbase/conf/hbase-site.xml /tmp/hbase-etc-hosts /tmp/hbase-hostname $SPARK_SSH:/tmp
        
        echo "SHC Connector: Copying hbase-site.xml to spark config folder with password... "
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no $SPARK_SSH -t $SCPhbasesiteCommandsToRun
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no -p 23 $SPARK_SSH -t $SCPhbasesiteCommandsToRun
        echo "SHC Connector: hbase-site.xml copy completed."

        echo "SHC Connector: Modifying /etc/hosts with HBase ip mapping with password... "
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no $SPARK_SSH -t $SCPetchostfileCommandsToRun
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no -p 23 $SPARK_SSH -t $SCPetchostfileCommandsToRun
        echo "SHC Connector: /etc/hosts modification completed."
    fi
}

Copy_by_hdfs()
{   
    # set timeout, if failed after 15 seconds print error msg, copy one file first to identify the first and only wn 
    # allowed to perform this the rest of the script
    echo "SHC Connector: Copying hbase IP mapping and hbase hostname to spark storage account... "
    sudo timeout 15 hdfs dfs -copyFromLocal /etc/hbase/conf/hbase-site.xml /tmp/hbase-etc-hosts /tmp/hbase-hostname \
        wasbs://$SPARK_STORAGE_CONTAINER@$SPARK_STORAGE_ACCOUNT.blob.core.windows.net/tmp/ && TIMED_OUT=false || TIMED_OUT=true
    if [ "$TIMED_OUT" = true ]; then
        echo "SHC Connector: File failed to copy from HBase to Spark cluster. File may already exist, or your Spark storage account may not have been added as HBase's secondary storage account."
        Remove_temporary_files_on_HBase_WN
        exit 1
    fi
    
    # remove testkey.txt previously scp-ed if using private key, move hbase-site to proper folder and change uid:gid
    HDFShbasesiteCommandsToRun="rm -f testkey.txt 
        && sudo hdfs dfs -copyToLocal -f /tmp/hbase-site.xml /etc/spark2/conf"
    
    # copy etc/hosts files from hbase to spark (remove old hbase ip mapping and add new hbase ip mapping)
    # remove any temporary files created
    HDFSetchostfileCommandsToRun="sudo cp /etc/hosts /tmp/etc-hosts-copy 
        && sudo hdfs dfs -copyToLocal -f /tmp/hbase-hostname /tmp/hbase-hostname
        && sudo cat /tmp/hbase-hostname | xargs -i sudo sed -i '/{}/d' /tmp/etc-hosts-copy
        && sudo bash -c 'hdfs dfs -cat /tmp/hbase-etc-hosts >> /tmp/etc-hosts-copy'
        && sudo cp /tmp/etc-hosts-copy /etc/hosts
        && sudo rm -f /tmp/hbase-hostname && sudo rm -f /tmp/etc-hosts-copy"

    HDFSRemoveTmpFiles="sudo hdfs dfs -rm /tmp/hbase-site.xml
        && sudo hdfs dfs -rm -f /tmp/hbase-hostname 
        && sudo hdfs dfs -rm -f /tmp/hbase-etc-hosts"

    if [ -n "${SSH_KEY_URL}" ]; then
        echo "SHC Connector: Copying hbase-site.xml to spark config folder with private key... "
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key $SPARK_SSH -t $HDFShbasesiteCommandsToRun
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key -p 23 $SPARK_SSH -t $HDFShbasesiteCommandsToRun
        echo "SHC Connector: hbase-site.xml copy completed."

        echo "SHC Connector: Modifying /etc/hosts with HBase ip mapping with private key... "
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key $SPARK_SSH -t $HDFSetchostfileCommandsToRun
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key -p 23 $SPARK_SSH -t $HDFSetchostfileCommandsToRun
        echo "SHC Connector: /etc/hosts modification completed."

        echo "SHC Connector: Remove HDFS temp files... "
        sudo ssh -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key $SPARK_SSH -t $HDFSRemoveTmpFiles
        echo "SHC Connector: tmp file removed."

    else
        echo "SHC Connector: Copying hbase-site.xml to spark config folder with password... "
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no $SPARK_SSH -t $HDFShbasesiteCommandsToRun
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no -p 23 $SPARK_SSH -t $HDFShbasesiteCommandsToRun
        echo "SHC Connector: hbase-site.xml copy completed."

        echo "SHC Connector: Modifying /etc/hosts with HBase ip mapping with password... "
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no $SPARK_SSH -t $HDFSetchostfileCommandsToRun
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no -p 23 $SPARK_SSH -t $HDFSetchostfileCommandsToRun
        echo "SHC Connector: /etc/hosts modification completed."

        echo "SHC Connector: Remove HDFS temp files... "
        sudo sshpass -p $SSH_PASSWORD ssh -o StrictHostKeyChecking=no $SPARK_SSH -t $HDFSRemoveTmpFiles
        echo "SHC Connector: tmp file removed."
    fi
}

# ============================= MAIN =======================================

echo "SHC Connector: Set up begins..."

# get script parameters
SPARK_CLUSTER_SUFFIX="azurehdinsight.net"
while getopts "u:n:s::a::c::p::k::" opt
do
   case "$opt" in
      u ) SPARK_SSH_USERNAME="$OPTARG" ;;
      n ) SPARK_CLUSTER_NAME="$OPTARG" ;;
      s ) SPARK_CLUSTER_SUFFIX="$OPTARG" ;;
      a ) SPARK_STORAGE_ACCOUNT="$OPTARG" ;;
      c ) SPARK_STORAGE_CONTAINER="$OPTARG" ;;
      p ) SSH_PASSWORD="$OPTARG" ;;
      k ) SSH_KEY_URL="$OPTARG" ;;
      ? ) 
        Help # Print helpFunction in case parameter is non-existent
        exit 0 
        ;; 
   esac
done

# check to make sure ssh user and cluster name are provided
if [[ -z "${SPARK_SSH_USERNAME}" || -z "${SPARK_CLUSTER_NAME}" ]]; then
   echo "SHC Connector: Both spark cluster SSH username and cluster name are required."
   Help
   exit 1
# check to make sure either password or private key url is provided
elif [[ -z "${SSH_PASSWORD}" && -z "${SSH_KEY_URL}" ]]; then
    echo "SHC Connector: Please provide authentication method to access the spark cluster. Specify '-p password' or '-k keyurl' as parameters to the script.\n"
    Help
    exit 1
else
    SPARK_SSH_HOST="${SPARK_CLUSTER_NAME}-ssh.${SPARK_CLUSTER_SUFFIX}"
    SPARK_SSH="${SPARK_SSH_USERNAME}@${SPARK_SSH_HOST}"
    echo "SHC TEST: ${SPARK_SSH_HOST} , ${SPARK_SSH}"

    # make sure only one wn is completing the communicating with spark (through hbase hdfs file edit)
    hostname > /tmp/lock
    sudo timeout 15 hdfs dfs -copyFromLocal /tmp/lock /tmp && FILE_EXISTS=false || FILE_EXISTS=true
    if [ "$FILE_EXISTS" = true ]; then
        echo "SHC Connector: connection to Spark head node will be performed by another worker node. Exiting..."
        sudo rm -f /tmp/lock
        exit 0

    # if selected as the wn to perform connection, check authentication method
    else
        echo "SHC Connector: this node $(hostname) selected to perform connection."
        if  [ -n "${SSH_KEY_URL}" ]; then
            wget -T 3 -O /tmp/sparkprivate.key $SSH_KEY_URL
            sudo chmod 700 /tmp/sparkprivate.key
            # check if this cluster use key as its authen method
            echo "test key" > /tmp/testkey.txt
            sudo scp -o StrictHostKeyChecking=no -i /tmp/sparkprivate.key /tmp/testkey.txt $SPARK_SSH:/home/$SPARK_SSH_USERNAME/ && RIGHT_KEY=true || RIGHT_KEY=false
            rm -f /tmp/testkey.txt
            if [ "$RIGHT_KEY" = false ]; then
                rm -f /tmp/sparkprivate.key
                echo "SHC Connector: Fail to connect to the Spark cluster with the provided key."
                sudo hdfs dfs -rm -f /tmp/lock
                exit 1
            fi
        else
            sudo apt-get install sshpass
        fi
    fi
fi

Get_HBase_Host_Mappings

# move files to spark storage & run commands on spark cluster
if [[ -n "${SPARK_STORAGE_CONTAINER}" && -n "${SPARK_STORAGE_ACCOUNT}" ]]; then
    Copy_by_hdfs
else
    Copy_by_scp
fi

# remove key & lock & tmp files if exists
Remove_temporary_files_on_HBase_WN

echo "SHC Connector: Set up completed successfully. "
exit 0