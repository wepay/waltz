#!/bin/sh

networkName=waltz-network

echo "----- creating docker network -----"
# check if waltz-network is there
networkId=$(docker network ls -q -f "name=${networkName}")
if [ "${networkId}" == "" ]
then
    echo "...network not found, creating..."
    docker network create $networkName
    networkId=$(docker network ls -q -f "name=${networkName}")
    echo "...network created [networkId=${networkName}]"
fi
