#!/bin/sh

networkName=waltz-network
die() { test -n "$*" && echo "$*"; exit 1; } >&2

# check if waltz-network already exists
networkId=$(docker network ls -q -f "name=${networkName}") || die
if test -z "${networkId}"; then
	echo "----- creating docker network $networkName -----"
	docker network create "$networkName" || die
fi
