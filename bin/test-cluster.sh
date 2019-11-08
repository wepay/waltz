#!/bin/sh

DIR=$(dirname $0)
cmd=$1

die() { printf "%s${1:+\n}" "$*"; exit 1; } >&2

start() {
    $DIR/docker/create-network.sh || die
    $DIR/docker/zookeeper.sh start || die
    $DIR/docker/cluster.sh create || die
    $DIR/docker/waltz-storage.sh start || die
    $DIR/docker/add-storage.sh || die
    $DIR/docker/waltz-server.sh start || die
}

stop() {
    $DIR/docker/waltz-server.sh stop || die
    $DIR/docker/waltz-storage.sh stop || die
    $DIR/docker/zookeeper.sh stop || die
}

clean() {
    $DIR/docker/waltz-server.sh clean || die
    $DIR/docker/waltz-storage.sh clean || die
    $DIR/docker/zookeeper.sh clean || die
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
        die "invalid command $cmd.  Valid commands are start, stop, restart, clean"
        ;;
esac
