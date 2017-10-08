#!/bin/bash

if [ -z "$VAR" ]
then
    ARCH='osx'
else
    ARCH=$1
fi

source activate bl-image-indexer
conda env export > environment_$ARCH.yml
