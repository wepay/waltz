#!/bin/sh

DIR=$(dirname $0)
cmd=$1

imageSource=waltz-storage:distDocker
imageName=com.wepay.waltz/waltz-storage
containerName="$2_storage"
configFolder="$2"

networkName=waltz-network
if [ $cmd = "start" ]; then
    portsOccupiedLowerBound=$3
    portsOccupiedUpperBound=$(($3 + 2))
    ports="$portsOccupiedLowerBound-$portsOccupiedUpperBound:$portsOccupiedLowerBound-$portsOccupiedUpperBound"
fi

runContainer() {
    local imageId=$(docker images -q ${imageName})
    if [ "${imageId}" = "" ]
    then
        echo "...image not built correctly"
    else
        docker run \
            --network=$networkName -p $ports --name $containerName -d -v $PWD/config/local-docker/$configFolder:/config/ \
            $imageId /config/waltz-storage.yml
    fi
}

source $DIR/container.sh
