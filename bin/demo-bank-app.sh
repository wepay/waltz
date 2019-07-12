#!/bin/sh

DIR=$(dirname $0)

$DIR/../gradlew --console plain -q copyLibs

CLASSPATH=""
for file in waltz-test/build/libs/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

JVMOPTS=-Dlog4j.configuration=file:config/log4j.properties

MAINCLASS=com.wepay.waltz.test.demo.DemoBankApp

java $JVMOPTS -cp ${CLASSPATH#:} $MAINCLASS config/local-docker/demo-app.yml
