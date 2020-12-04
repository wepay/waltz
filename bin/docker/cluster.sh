#!/bin/sh

DIR=$(dirname $0)
cmd=$1

$DIR/../../gradlew --console=plain -q copyLibs

CLASSPATH=""
for file in waltz-tools/build/libs/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

JVMOPTS=-Dlog4j.configuration=file:config/log4j.properties

TOOLSCONFIG=config/local-docker/waltz-tools.yml
ZKCLI=com.wepay.waltz.tools.zk.ZooKeeperCli

numPartitions=${WALTZ_TEST_CLUSTER_NUM_PARTITIONS:-1}
clusterKey=$(java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI show-cluster-key -c $TOOLSCONFIG)

case $cmd in
    create)
        if [ "${clusterKey}" != "" ]
        then
            echo "----- cluster already created -----"
        else
            echo "----- creating a cluster -----"

            java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI create -c $TOOLSCONFIG -n waltz_cluster -p $numPartitions
            echo "waltz cluster created"
        fi

        mkdir -p build/config

        cp config/local-docker/waltz-server.yml build/config/waltz-server.yml
        cp config/local-docker/waltz-storage.yml build/config/waltz-storage.yml

        echo "config files are generated in build/config"
        ;;

    delete)
        if [ "$clusterKey" != "" ]
        then
            java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI delete -c $TOOLSCONFIG -n waltz_cluster
            echo "cluster deleted"
        fi
        ;;
    *)
        echo "unknown command"
        ;;
esac


