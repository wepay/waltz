#!/bin/sh

DIR=$(dirname $0)
cmd=$1
serverPortBase=55180
storagePortBase=55280
storagePortsOccupied=3
serverPortsOccupied=3

start() {
    set -e
    numWaltzClusters=$1
    $DIR/docker/create-network.sh
    $DIR/docker/zookeeper.sh start
    for clusterNumber in $(seq 0 $(($numWaltzClusters - 1))); do
        initiateCluster $clusterNumber
        echo "----- Cluster $clusterNumber created!"
    done
}

initiateCluster() {
    clusterNumber=$1
    serverPortLowerBound=$((serverPortBase + $clusterNumber * $serverPortsOccupied))
    storagePortLowerBound=$(($storagePortBase + $clusterNumber * $storagePortsOccupied))
    $DIR/docker/cluster-config-files.sh $clusterNumber $serverPortLowerBound $storagePortLowerBound

    $DIR/docker/cluster.sh create $clusterNumber
    $DIR/docker/waltz-storage.sh start "$clusterNumber" $storagePortLowerBound $(($storagePortLowerBound + $storagePortsOccupied - 1)) 0
    $DIR/docker/add-storage.sh $clusterNumber $storagePortLowerBound
    $DIR/docker/waltz-server.sh start $clusterNumber $serverPortLowerBound $(($serverPortLowerBound + $serverPortsOccupied - 1))
}

stop() {
    for clusterNumber in $(docker container ls --format '{{.Names}}' --filter "name=waltz-server-*" | sed 's/^.*-//'); do
        stopCluster $clusterNumber
    done
    $DIR/docker/zookeeper.sh stop
}

stopCluster() {
    $DIR/docker/waltz-server.sh stop $1
    $DIR/docker/waltz-storage.sh stop $1
}

clean() {
    for clusterNumber in $(docker container ls -a --format '{{.Names}}' --filter "name=waltz-server-*" | sed 's/^.*-//'); do
        cleanCluster $clusterNumber
    done
    $DIR/docker/zookeeper.sh clean
}

cleanCluster() {
    $DIR/docker/waltz-server.sh clean $1
    $DIR/docker/waltz-storage.sh clean $1
    rm -r $DIR/../build/config-$1
}

addCluster() {
    # The total count of all clusters is also the name of the newly created cluster
    count=$(docker container ls -a --format '{{.Names}}' --filter "name=waltz-server-*" | wc -l)
    initiateCluster $count
    echo "----- Cluster $count added"
}

case $cmd in
    start)
        if [ "$#" -ne 2 ]; then
            echo "Usage: $0 $1 Number_of_clusters_to_be_created" >&2
            exit 1
        fi
        start $2
        ;;
    stop)
        stop
        ;;
    restart)
        count=$(docker container ls -a --format '{{.Names}}' --filter "name=waltz-server-*" | wc -l)
        stop
        start $count
        ;;
    clean)
        clean
        ;;
    addCluster)
        clusterNumber=$(docker container ls --format '{{.Names}}' --filter "name=waltz-server-*" | wc -l)
        if [ "$#" -eq 2 ]; then
            clusterNumber=$2
        fi
        addCluster $2
        ;;
    stopCluster)
        if [ "$#" -ne 2 ]; then
            echo "Usage: $0 $1 Number of a cluster to be stopped" >&2
            exit 1
        fi
        stopCluster $2
        ;;
    cleanCluster)
        if [ "$#" -ne 2 ]; then
            echo "Usage: $0 $1 Number of a cluster to be removed" >&2
            exit 1
        fi
        cleanCluster $2
        ;;
    restartCluster)
        if [ "$#" -ne 2 ]; then
            echo "Usage: $0 $1 Cluster to be restarted" >&2
            exit 1
        fi
        stopCluster $2
        initiateCluster $2
        ;;
    *)
        echo "invalid command"
        ;;
esac
