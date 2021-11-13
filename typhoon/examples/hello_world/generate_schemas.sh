#!/usr/bin/env bash
export LC_ALL=en_US.utf-8
export LANG=en_US.utf-8
export TYPHOON_HOME=$1

source ~/.virtualenvs/typhoon/bin/activate
typhoon generate-json-schemas