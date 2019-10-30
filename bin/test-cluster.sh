#!/bin/sh

DIR=$(dirname $0)
cmd=$1

die() { test -n "$*" && echo "$*"; exit 1; } >&2

start() {
    $DIR/docker/create-network.sh || die
    $DIR/docker/zookeeper.sh start || die
    $DIR/docker/cluster.sh create || die
    $DIR/docker/waltz-storage.sh start || die
    $DIR/docker/add-storage.sh || die
    $DIR/docker/waltz-server.sh start || die
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
