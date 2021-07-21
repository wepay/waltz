#!/bin/sh

DIR=$(dirname $0)
cmd=$1
defaultServerPortBase=55180
defaultStoragePortBase=55280
storagePortsOccupied=3
serverPortsOccupied=3

# supported commands: start/stop/restart/clean
# test-cluster.sh start <- initiate network, add a default cluster
# test-cluster.sh start <cluster_name> <- starts again already created cluster on the same ports
# test-cluster.sh start <cluster_name> <base_server_port> <base_storage_port> <- initiate network,
# adds new cluster of one storage & server node running on provided ports
# test-cluster.sh stop (cluster_name) <- stop all clusters/(stop cluster with given cluster name)
# test-cluster.sh restart (cluster_name) <- restart all clusters/(restart cluster with given cluster name)
# test-cluster.sh clean (cluster_name) <- remove all clusters/(remove cluster with given cluster name)

initNetwork() {
    $DIR/docker/create-network.sh
    $DIR/docker/zookeeper.sh start
}

getContainerPort() {
    containerName=$1
    containerExists=$(docker container ls -a --format '{{.Names}}' --filter "name=$containerName" | wc -l)
    if [ "$containerExists" -gt 0 ]; then
        containerId=$(docker container ls -a --format '{{.ID}}' --filter "name=$containerName" | head -1)
        assignedPort=$(docker inspect --format='{{.HostConfig.PortBindings}}' $containerId | grep -o -E '[0-9]+' | head -1)
        echo "$assignedPort"
    else
        echo "docker container $containerName not found"
        exit 1
    fi
}

rerun() {
    clusterName=$1
    serverPortBase=$(getContainerPort "${clusterName}_server")
    storePortBase=$(getContainerPort "${clusterName}_store")
    startCluster $clusterName $serverPortBase $storePortBase
}

startCluster() {
    clusterName=$1
    serverPortLowerBound="$2"
    storagePortLowerBound="$3"
    echo "----- Creating $clusterName cluster"
    $DIR/docker/cluster-config-files.sh "$clusterName" $serverPortLowerBound $storagePortLowerBound

    $DIR/docker/cluster.sh create "$clusterName"
    $DIR/docker/waltz-storage.sh start "$clusterName" $storagePortLowerBound $(($storagePortLowerBound + $storagePortsOccupied - 1))
    $DIR/docker/add-storage.sh "$clusterName" $storagePortLowerBound
    $DIR/docker/waltz-server.sh start "$clusterName" $serverPortLowerBound $(($serverPortLowerBound + $serverPortsOccupied - 1))
    echo "----- Cluster $clusterName created!"
}

stopCluster() {
    $DIR/docker/waltz-server.sh stop "$1"
    $DIR/docker/waltz-storage.sh stop "$1"
    echo "----- Cluster $clusterName stopped!"
}

stop() {
    for clusterName in $(docker container ls --format '{{.Names}}' --filter "name=waltz_.*" | sed 's/_server//; s/_store//' | uniq); do
        echo $clusterName
        stopCluster "$clusterName"
    done
    $DIR/docker/zookeeper.sh stop
}

clean() {
    for clusterName in $(docker container ls -a --format '{{.Names}}' --filter "name=waltz_.*" | sed 's/_server//; s/_store//' | uniq); do
        cleanCluster "$clusterName"
    done
    $DIR/docker/zookeeper.sh clean
}

cleanCluster() {
    $DIR/docker/waltz-server.sh clean "$1"
    $DIR/docker/waltz-storage.sh clean "$1"
    rm -r $DIR/../config/local-docker/"$1"
    echo "----- Cluster $clusterName cleaned!"
}

case $cmd in
    start)
        set -e
        if [ "$#" -eq 1 ]; then
            initNetwork
            startCluster "waltz_cluster" "$defaultServerPortBase" "$defaultStoragePortBase"
        elif [ "$#" -eq 2 ]; then
            containerExists=$(docker container ls -a --format '{{.Names}}' --filter "name=waltz_$2.*" | sed 's/_server//; s/_store//' | uniq | wc -l)
            if [ $containerExists -ne 1 ]; then
                echo "Missing cluster with name waltz_$2. No attempt to start cluster again at the same port numbers."
                exit 1
            fi
            rerun "waltz_$2"
        elif [ "$#" -eq 4 ]; then
            containerExists=$(docker container ls -a --format '{{.Names}}' --filter "name=waltz_$2.*" | wc -l)
            if [ $containerExists -gt 0 ]; then
                echo "Cluster waltz_$2 already exists. Please create another one or perform restart"
                exit 1
            fi
            initNetwork
            startCluster "waltz_$2" "$3" "$4"
        else
            echo "Usage: test-cluster.sh start <cluster_name> <server_base_port> <storage_base_port>"
        fi
        ;;
    stop)
        if [ "$#" -eq 2 ]; then
            stopCluster "waltz_$2"
        else
            stop
        fi
        ;;
    restart)
        if [ "$#" -eq 2 ]; then
            clusterName="waltz_$2"
            stopCluster "$clusterName"
            rerun "$clusterName"
        else
            stop
            initNetwork
            for clusterName in $(docker container ls -a --format '{{.Names}}' --filter "name=waltz_.*" | sed 's/_server//; s/_store//' | uniq); do
                rerun "$clusterName"
            done
        fi
        ;;
    clean)
        if [ "$#" -eq 2 ]; then
            cleanCluster "waltz_$2"
        else
            clean
        fi
        ;;
    *)
        echo "invalid command"
        ;;
esac
