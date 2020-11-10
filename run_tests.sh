#!/usr/bin/env bash

pipver="$(pip list | grep fs.anvilfs | awk '{print $2}')"
repover="$( cat $(dirname $0)/anvilfs/__about__.py | grep '__version__' | cut -d '"' -f2)"
if [[ "$pipver" != "$repover" ]] ; then
    printf "AnVILFS TEST ERROR:\n\tpip version $pipver != repo version $repover\n"
    exit
else
    cd $(dirname $0)/
    pytest .
fi
