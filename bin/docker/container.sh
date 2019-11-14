die() { printf "%s${1:+\n}" "$*"; exit 1; } >&2

findImage() {
    # pull or build an image, as necessary
    local imageId
    imageId=$(docker images -q ${imageName}) || die
    if [ "${imageId}" == "" ]
    then
        if [ $imageSource == "docker" ]
        then
            echo "...pulling image [$imageName]"
            docker pull $imageName || die
            imageId=$(docker images -q ${imageName})
            echo "...image pulled [$imageId]"
        else
            echo "...image not found, building [$imageName]"
            $DIR/../../gradlew $imageSource || die
            echo "...image built [$imageName]"
        fi
    fi
}

startContainer() {
    # start a container if not already running
    local containerId
    containerId=$(docker ps -q -f name=${containerName}) || die
    if [ "${containerId}" != "" ]
    then
        echo "...container already running [$containerName]"
    else
        # check if the container is stopped
        containerId=$(docker ps -q -a -f name=${containerName}) || die
        if [ "${containerId}" != "" ]
        then
            echo "...resuming container [$containerId]"
            docker start $containerName || die
        else
            echo "...starting container [$containerName]"
            runContainer || die
        fi
    fi
}

stopContainer() {
    # stop container, if running
    local containerId
    containerId=$(docker ps -q -f name=${containerName}) || die
    if [ "${containerId}" != "" ]
    then
        echo "...stopping $containerName [$containerId]"
        docker stop $containerId || die
    else
        echo "...container already stopped [$containerName]"
    fi
}

removeContainer() {
    # remove container if not running
    local containerId
    containerId=$(docker ps -q -f name=${containerName}) || die
    if [ "${containerId}" != "" ]
    then
        echo "...container still running [$containerName], not removed"
    else
        containerId=$(docker ps -q -a -f name=${containerName}) || die
        if [ "${containerId}" != "" ]
        then
            echo "...Removing container $containerName [$containerId]"
            docker rm "$containerName" || die
        else
            echo "...container not found [$containerName]"
        fi
    fi
}

case $cmd in
    start)
        echo "---- starting $containerName -----"
        findImage
        startContainer
        ;;

    stop)
        echo "---- stopping $containerName -----"
        stopContainer
        ;;

    clean)
        echo "---- removing $containerName -----"
        removeContainer
        ;;
    *)
        die "invalid command [$cmd].  Valid commands: start, stop, clean"
        ;;
esac
