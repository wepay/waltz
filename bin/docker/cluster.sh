#!/bin/sh

DIR=$(dirname $0)
cmd=$1
clusterName=$2

$DIR/../../gradlew --console=plain -q copyLibs

CLASSPATH=""
for file in waltz-tools/build/libs/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

JVMOPTS=-Dlog4j.configuration=file:config/log4j.properties

TOOLSCONFIG=config/local-docker/"$clusterName"/waltz-tools.yml
ZKCLI=com.wepay.waltz.tools.zk.ZooKeeperCli

numPartitions=${WALTZ_TEST_CLUSTER_NUM_PARTITIONS:-1}
clusterKey=$(java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI show-cluster-key -c $TOOLSCONFIG)

case $cmd in
    create)
        if [ "${clusterKey}" != "" ]
        then
            echo "----- cluster $clusterName already created -----"
        else
            echo "----- creating $clusterName cluster -----"

            java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI create -c $TOOLSCONFIG -n "$clusterName" -p $numPartitions
            echo "$clusterName cluster created"
        fi
        ;;

    delete)
        if [ "$clusterKey" != "" ]
        then
            java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI delete -c $TOOLSCONFIG -n "$clusterName"
            echo "cluster deleted"
        fi
        ;;
    *)
        echo "unknown command"
        ;;
esac


