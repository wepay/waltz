#!/bin/sh

DIR=$(dirname $0)

$DIR/../gradlew --console plain -q copyLibs

export CLASSPATH=$(echo waltz-demo/build/libs/*.jar | tr ' ' :)
export JAVA_TOOL_OPTIONS=-Dlog4j.configuration=file:config/log4j.properties

java com.wepay.waltz.demo.DemoBankApp config/local-docker/demo-app.yml
