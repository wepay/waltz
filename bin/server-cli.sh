#!/bin/sh

DIR=$(dirname $0)

$DIR/../gradlew --console plain -q copyLibs

CLASSPATH=""
for file in waltz-tools/build/libs/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

JVMOPTS=-Dlog4j.configuration=file:config/log4j.properties

MAINCLASS=com.wepay.waltz.tools.server.ServerCli

java $JVMOPTS -cp ${CLASSPATH#:} $MAINCLASS $@
