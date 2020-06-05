#!/usr/bin/env bash
set -e
mkdir -p /pv/logs
if [ -n "$BOOTSTRAP_LEADER" ];then
  echo "$(hostname) is configured as leader"
  echo "Starting Shard Services"
  kv -i "$(hostname)" -l :17001 -r "$(hostname)":18001 -d "$(hostname)/shard-0" > /pv/logs/shard0.log 2>&1 &
  kv -i "$(hostname)" -l :17002 -r "$(hostname)":18002 -d "$(hostname)/shard-1" > /pv/logs/shard1.log 2>&1 &

  echo "Starting Co-ordination Service"
  kv -i "$(hostname)" -c -l :17000 -r "$(hostname)":18000 -d "$(hostname)" > /pv/logs/coord.log 2>&1 &
elif [ -n "$BOOTSTRAP_FOLLOWER" ]; then
  echo "$(hostname) is configured as follower"
  echo "Starting Shard Services"
  sleep 5
  kv -i "$(hostname)" -l :17001 -r "$(hostname)":18001 -j node0:17001 -d "$(hostname)/shard-0" > /pv/logs/"$(hostname)-shard0.log" 2>&1 &
  kv -i "$(hostname)" -l :17002 -r "$(hostname)":18002 -j node0:17002 -d "$(hostname)/shard-1" > /pv/logs/"$(hostname)-shard1.log" 2>&1 &

  echo "Starting Co-ordination Service"
  kv -i "$(hostname)" -c -l :17000 -r "$(hostname)":18000 -j node0:17000 -d "$(hostname)" > /pv/logs/coord.log 2>&1 &

fi
while true
do
  sleep 1
done
