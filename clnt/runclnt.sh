#!/bin/bash
export ZOOBINDIR=~/apache-zookeeper-3.6.2-bin/bin
if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

# TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=lab2-10.cs.mcgill.ca:21834,lab2-11.cs.mcgill.ca:21834,lab2-13.cs.mcgill.ca:21834

java -cp $CLASSPATH:../task:.: DistClient "$@"
