#!/bin/sh

DIR=$(dirname $0)
cmd=$1

imageSource=waltz-server:distDocker
imageName=com.wepay.waltz/waltz-server
containerName=waltz-server
networkName=waltz-network
ports=55180-55182:55180-55182

runContainer() {
    local imageId
    imageId=$(docker images -q ${imageName}) || die
    if [ "${imageId}" == "" ]
    then
        echo "...image not built correctly"
    else
        docker run \
            --network=$networkName --net-alias $(hostname) -h $(hostname) -p $ports \
            --name $containerName -d -v $PWD/build/config:/config/ \
            $imageId /config/waltz-server.yml || die
    fi
}

source $DIR/container.sh
