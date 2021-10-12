#! /bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

cd ./data/$1

start=`date +%s`
eve 1 1 | getmodel | cdftocsv > dump.csv
end=`date +%s`

RUNTIME=$((end-start))
echo $RUNTIME
