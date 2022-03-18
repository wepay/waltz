#!/bin/sh

DIR=$(dirname $0)
cmd=$1

imageSource=docker
tag=3.4
imageName=zookeeper:$tag
containerName=zk
networkName=waltz-network
ports=42181:2181
platform=linux/amd64

runContainer() {
    docker run --network=$networkName -p $ports --platform $platform --name $containerName -d $imageName
}

source $DIR/container.sh
