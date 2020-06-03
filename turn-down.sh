#!/bin/bash

function usage () {
cat <<HELP_USAGE
Usage: $0  -n <node> -t <timeout> -r
-n    Name of the node
-t    Time to recovery, if missing never recovered
-r    If set, recovered with restarting
HELP_USAGE
}

node=""
timeout=-100
restart=false

while getopts ":n:t:r" opt; do
    case $opt in
        t) timeout=$OPTARG;;
        r) restart=true;;
        n) node=$OPTARG;;
        \?) usage;exit 1;;
        *) break ;;
    esac
done
shift $((OPTIND -1))


if [ "$node" == "" ]; then
  echo "-n <node> is missing"
  usage
  exit 1;
fi

echo "Pause $node..."
docker pause "$node"

secs=$(($timeout))
while [ $secs -gt 0 ]; do
   # countdown on the same line
   echo -ne "Resume $node in $secs s...\033[0K\r"
   sleep 1
   : $((secs--))
done

echo ""

if [ $secs == -100 ] ; then
  exit 1
fi

if [ "$restart" = true ] ; then
  echo "Restarting $node..."
  docker restart "$node"
else
  echo "Unpausing $node..."
  docker unpause "$node"
fi





