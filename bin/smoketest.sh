#!/bin/sh

DIR=$(dirname $0)

$DIR/../gradlew --console plain -q copyLibs

unset CLASSPATH
for file in waltz-test/build/libs/*.jar; do
    CLASSPATH="${CLASSPATH+$CLASSPATH:}$file"
done

JVMOPTS="-Xms1g -Xmx1g -server -Dlog4j.configuration=file:config/log4j-smoketest.properties"

MAINCLASS=com.wepay.waltz.test.smoketest.SmokeTest

java $JVMOPTS -cp ${CLASSPATH#:} $MAINCLASS 2>$HOME/smoketest.log
