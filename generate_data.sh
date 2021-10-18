#!/usr/bin/env bash

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

oasislmf test model generate-oasis-files -C ./configs/$1_config.json

DATA_PATH="$( for dir in ./runs/*; do echo "$dir"; done)"

FILE_NAME="$(basename ${DATA_PATH})"
echo $DATA_PATH

cp -R $DATA_PATH/ ./data/

rm -r ./runs
cd data
mv ./$FILE_NAME ./$1
cd $1

mkdir input && cp events.bin ./input/events.bin
mkdir static
cp footprint.bin ./static/footprint.bin
cp items.bin ./input/items.bin
cp vulnerability.bin ./static/vulnerability.bin
cp damage_bin_dict.bin ./static/damage_bin_dict.bin
cp footprint.idx ./static/footprint.idx

# generate csv files
evetocsv < ./input/events.bin > ./input/events.csv
footprinttocsv < ./static/footprint.bin > ./static/footprint.csv
footprinttocsv < ./input/footprint.bin > ./input/footprint.csv
itemstocsv < ./input/items.bin > ./input/items.csv
vulnerabilitytocsv < ./static/vulnerability.bin > ./static/vulnerability.csv
damagebintocsv < ./static/damage_bin_dict.bin > ./static/damage_bin_dict.csv
