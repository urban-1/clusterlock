#!/bin/bash

trap ctrl_c INT
function ctrl_c() {
    echo "** Trapped CTRL-C"
    # Kill all micro-services
    kill `ps -ef | grep python | grep lock.py | awk -F' +' '{print $2}'`
    exit 0
}

for i in {1..24}
do
    echo "Spawning $i"
    ./lock.py "job-$i" &
done

while [ 1 ]
do
    sleep 10
done