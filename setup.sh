#! /bin/bash


if [ ! -d "./data" ]
then
	mkdir ./data
fi

if [ ! -d "./configs" ]
then
	mkdir ./configs
fi

if [ ! -d "./venv" ]
then 
    python3 -m venv venv 
    source venv/bin/activate
    pip install -r requirements.txt
fi
