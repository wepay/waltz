#!/bin/sh

DIR=$(dirname $0)
cmd=$1
serverPortBase=55180
storagePortBase=55280
storagePortsOccupied=3
serverPortsOccupied=3
defaultClusterName="default"

# supported commands: start/stop/restart/clean
# test-cluster.sh start <- initiate network, add a default cluster
# test-cluster.sh start name1 name2 name3<- initiate network, adds new clusters with the name specified
# test-cluster.sh stop (nameX) <- stop all clusters/(stop cluster with given cluster number)
# test-cluster.sh restart (nameX) <- restart all clusters/(restart cluster with given cluster number)
# test-cluster.sh clean (name1) <- remove all clusters/(remove cluster with given cluster number)

initNetwork() {
    $DIR/docker/create-network.sh
    $DIR/docker/zookeeper.sh start
}

getClusterId() {
    clusterName=$1
    clusterIdNew=$(docker container ls -a --format '{{.Names}}' --filter "name=waltz_ledger_server_*" | wc -l)
    clusterExists=$(docker container ls -a --format '{{.Names}}' --filter "name=waltz_ledger_server_$clusterName" | wc -l)
    if [[ "$clusterExists" -gt 0 ]]; then
        containerId=$(docker container ls -a --format '{{.ID}}' --filter "name=waltz_ledger_server_$clusterName" | head -1)
        assignedPort=$(docker inspect --format='{{.HostConfig.PortBindings}}' $containerId | grep -o -E '[0-9]+' | head -1)
        echo $((($assignedPort - $serverPortBase) / $serverPortsOccupied))
    else
        echo $clusterIdNew
    fi
}

startCluster() {
    clusterName=$1
    clusterId=$(getClusterId $clusterName)
    echo "----- Creating waltz_ledger_$clusterName cluster"
    serverPortLowerBound=$(($serverPortBase + $clusterId * $serverPortsOccupied))
    storagePortLowerBound=$(($storagePortBase + $clusterId * $storagePortsOccupied))
    if [ "$clusterName" = "$defaultClusterName" ]; then
        $DIR/docker/cluster-config-files.sh "$clusterName" $serverPortLowerBound $storagePortLowerBound ""
    else
        $DIR/docker/cluster-config-files.sh "$clusterName" $serverPortLowerBound $storagePortLowerBound "_$clusterName"
    fi

    $DIR/docker/cluster.sh create "$clusterName"
    $DIR/docker/waltz-storage.sh start "$clusterName" $storagePortLowerBound $(($storagePortLowerBound + $storagePortsOccupied - 1)) 0
    $DIR/docker/add-storage.sh "$clusterName" $storagePortLowerBound
    $DIR/docker/waltz-server.sh start "$clusterName" $serverPortLowerBound $(($serverPortLowerBound + $serverPortsOccupied - 1))
    echo "----- Cluster waltz_ledger_$clusterName created!"
}

stopCluster() {
    $DIR/docker/waltz-server.sh stop "$1"
    $DIR/docker/waltz-storage.sh stop "$1"
}

stop() {
    for clusterId in $(docker container ls --format '{{.Names}}' --filter "name=waltz_ledger_server_*" | sed 's/^.*server_//'); do
        stopCluster "$clusterId"
    done
    $DIR/docker/zookeeper.sh stop
}

clean() {
    for clusterId in $(docker container ls -a --format '{{.Names}}' --filter "name=waltz_ledger_server_*" | sed 's/^.*server_//'); do
        cleanCluster "$clusterId"
    done
    $DIR/docker/zookeeper.sh clean
}

cleanCluster() {
    $DIR/docker/waltz-server.sh clean "$1"
    $DIR/docker/waltz-storage.sh clean "$1"
    rm -r $DIR/../build/config-"$1"
}

case $cmd in
    start)
        set -e
        if [ "$#" -eq 1 ]; then
            initNetwork
            startCluster "$defaultClusterName"
        else
            initNetwork
            for clusterName in "${@:2}"; do
                startCluster "$clusterName"
            done
        fi
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
            stop
            initNetwork
            for clusterName in $(docker container ls -a --format '{{.Names}}' --filter "name=waltz_ledger_server_*" | sed 's/^.*server_//'); do
                startCluster $clusterName
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
