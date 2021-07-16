#!/bin/sh

DIR=$(dirname $0)
cmd=$1
serverPortBase=55180
storagePortBase=55280
storagePortsOccupied=3
serverPortsOccupied=3

# supported commands: start/stop/restart/clean
# test-cluster.sh start <- initiate network, add 1 new cluster
# test-cluster.sh stop (0) <- stop all clusters/(stop cluster with given cluster number)
# test-cluster.sh restart (0) <- restart all clusters/(restart cluster with given cluster number)
# test-cluster.sh clean (0) <- remove all clusters/(remove cluster with given cluster number)

initNetwork() {
    $DIR/docker/create-network.sh
    $DIR/docker/zookeeper.sh start
}

startCluster() {
    clusterId=$1
    echo $clusterId
    serverPortLowerBound=$((serverPortBase + $clusterId * $serverPortsOccupied))
    storagePortLowerBound=$(($storagePortBase + $clusterId * $storagePortsOccupied))
    $DIR/docker/cluster-config-files.sh $clusterId $serverPortLowerBound $storagePortLowerBound

    $DIR/docker/cluster.sh create $clusterId
    $DIR/docker/waltz-storage.sh start "$clusterId" $storagePortLowerBound $(($storagePortLowerBound + $storagePortsOccupied - 1)) 0
    $DIR/docker/add-storage.sh $clusterId $storagePortLowerBound
    $DIR/docker/waltz-server.sh start $clusterId $serverPortLowerBound $(($serverPortLowerBound + $serverPortsOccupied - 1))
    echo "----- Cluster $clusterId created!"
}

stop() {
    for clusterId in $(docker container ls --format '{{.Names}}' --filter "name=waltz-server-*" | sed 's/^.*-//'); do
        stopCluster $clusterId
    done
    $DIR/docker/zookeeper.sh stop
}

stopCluster() {
    $DIR/docker/waltz-server.sh stop $1
    $DIR/docker/waltz-storage.sh stop $1
}

clean() {
    for clusterId in $(docker container ls -a --format '{{.Names}}' --filter "name=waltz-server-*" | sed 's/^.*-//'); do
        cleanCluster $clusterId
    done
    $DIR/docker/zookeeper.sh clean
}

cleanCluster() {
    $DIR/docker/waltz-server.sh clean $1
    $DIR/docker/waltz-storage.sh clean $1
    rm -r $DIR/../build/config-$1
}

case $cmd in
    start)
        set -e
        #
        clusterId=$(docker container ls --format '{{.Names}}' --filter "name=waltz-server-*" | wc -l)
        initNetwork
        startCluster $clusterId
        ;;
    stop)
        if [ "$#" -eq 2 ]; then
            stopCluster $2
        else
            stop
        fi
        ;;
    restart)
        if [ "$#" -eq 2 ]; then
            stopCluster $2
            startCluster $2
        else
            count=$(docker container ls -a --format '{{.Names}}' --filter "name=waltz-server-*" | wc -l)
            stop
            for clusterId in $($count); do
                startCluster $clusterId
            done
        fi
        ;;
    clean)
        if [ "$#" -eq 2 ]; then
            cleanCluster $2
        else
            clean
        fi
        ;;
    *)
        echo "invalid command"
        ;;
esac
