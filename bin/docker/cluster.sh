#!/bin/sh

DIR=$(dirname $0)
cmd=$1
ending=$2

$DIR/../../gradlew --console=plain -q copyLibs

CLASSPATH=""
for file in waltz-tools/build/libs/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

JVMOPTS=-Dlog4j.configuration=file:config/log4j.properties

TOOLSCONFIG=build/config-"$ending"/waltz-tools.yml
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

            java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI create -c $TOOLSCONFIG -n waltz_cluster_"$ending" -p $numPartitions
            echo "waltz cluster created"
        fi
        ;;

    delete)
        if [ "$clusterKey" != "" ]
        then
            java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI delete -c $TOOLSCONFIG -n waltz_cluster_"$ending"
            echo "cluster deleted"
        fi
        ;;
    *)
        echo "unknown command"
        ;;
esac


