#! /bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

cd ./data/$1

#if [ ! "./OasisLMF" ]
#then
#	git clone https://github.com/OasisLMF/OasisLMF.git
#	cd OasisLMF
#	git checkout pymodel-optimiza
#	cd ..
#fi

#git clone https://github.com/OasisLMF/OasisLMF.git
#cd OasisLMF
#git checkout pymodel-optimize
#cd ..

start=`date +%s`
eve 1 1 | getpymodel > /dev/null
end=`date +%s`

RUNTIME=$((end-start))
echo $RUNTIME

