#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Which application should I spawn:"
    echo "Usage:"
    echo "   $0 <app>"
    echo 
    exit 1
fi
app="$1"
if [ $# -gt 1 ]; then
    app="$app $2"
fi


trap ctrl_c INT
function ctrl_c() {
    echo "** Trapped CTRL-C"
    # Kill all micro-services
    ids=`ps -ef | grep python | grep "$app" | egrep -v "(spawn|grep)" |  awk -F' +' '{printf "%s ",$2}'`
    echo "kill $ids"
    kill $ids
    exit 0
}

for i in {1..1}
do
    echo "Spawning $i"
    $app "job-$i" &
done

while [ 1 ]
do
    sleep 10
done