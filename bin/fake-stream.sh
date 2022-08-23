#! /usr/bin/env bash
input=$1
lines=$2
interval=$3
outputdir=$4

function handler() 
{
  rm -f $outputdir/*.log
  exit 1
}

trap 'handler' SIGINT

i=1
while :; do
   cat $input | shuf | head -$lines > $outputdir/$i.log
   i=$((i+1))
   sleep $interval
done
