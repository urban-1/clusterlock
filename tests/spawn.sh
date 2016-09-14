#!/bin/bash

app="$1"


trap ctrl_c INT
function ctrl_c() {
    echo "** Trapped CTRL-C"
    # Kill all micro-services
    kill `ps -ef | grep python | grep "$app" | awk -F' +' '{print $2}'`
    exit 0
}

for i in {1..10}
do
    echo "Spawning $i"
    $app "job-$i" &
done

while [ 1 ]
do
    sleep 10
done