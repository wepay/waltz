#!/bin/sh

DIR=$(dirname $0)
cmd=$1

imageSource=waltz-storage:distDocker
imageName=com.wepay.waltz/waltz-storage
containerName=waltz-storage
networkName=waltz-network
ports=55280-55282:55280-55282

runContainer() {
    local imageId
    imageId=$(docker images -q ${imageName}) || die
    if [ "${imageId}" == "" ]
    then
        echo "...image not built correctly"
    else
        docker run \
            --network=$networkName -p $ports --name $containerName -d -v $PWD/build/config:/config/ \
            $imageId /config/waltz-storage.yml || die
    fi
}

source $DIR/container.sh
