#!/bin/sh

DIR=$(dirname $0)

$DIR/../gradlew --console plain -q copyLibs

unset CLASSPATH
unset JAVA_TOOL_OPTIONS
CLASSPATH=$(echo waltz-demo/build/libs/*.jar | tr ' ' :)
JAVA_TOOL_OPTIONS=-Dlog4j.configuration=file:config/log4j.properties

java \
	$JAVA_TOOL_OPTIONS \
	-cp "$CLASSPATH" \
	com.wepay.waltz.demo.DemoBankApp config/local-docker/demo-app.yml
