#!/bin/sh

DIR=$(dirname $0)

$DIR/../../gradlew --console=plain -q copyLibs

CLASSPATH=""
for file in waltz-tools/build/libs/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

JVMOPTS=-Dlog4j.configuration=file:config/log4j.properties

TOOLSCONFIG=config/local-docker/waltz-tools.yml
ZKCLI=com.wepay.waltz.tools.zk.ZooKeeperCli
STORAGECLI=com.wepay.waltz.tools.storage.StorageCli

echo "----- adding a storage node to the cluster -----"

java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI add-storage-node -c $TOOLSCONFIG -s waltz-storage:55280 -a 55281 -g 0
echo "...a storage node added the cluster"

java $JVMOPTS -cp ${CLASSPATH#:} $ZKCLI assign-partition -c $TOOLSCONFIG -s waltz-storage:55280 -p 0
echo "...a partition assigned to the storage node"

until nc -z localhost 55281; do echo "Waiting for Waltz storage to start..."; sleep 1; done

java $JVMOPTS -cp ${CLASSPATH#:} $STORAGECLI add-partition -c $TOOLSCONFIG -s localhost:55281 -p 0
echo "...a partition added to the storage"
