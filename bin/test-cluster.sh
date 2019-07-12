#!/bin/sh

DIR=$(dirname $0)
cmd=$1

start() {
    $DIR/docker/create-network.sh
    $DIR/docker/zookeeper.sh start
    $DIR/docker/cluster.sh create
    $DIR/docker/waltz-storage.sh start
    $DIR/docker/add-storage.sh
    $DIR/docker/waltz-server.sh start
}

stop() {
    $DIR/docker/waltz-server.sh stop
    $DIR/docker/waltz-storage.sh stop
    $DIR/docker/zookeeper.sh stop
}

clean() {
    $DIR/docker/waltz-server.sh clean
    $DIR/docker/waltz-storage.sh clean
    $DIR/docker/zookeeper.sh clean
}

case $cmd in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
    clean)
        clean
        ;;
    *)
        echo "invalid command"
        ;;
esac
