#!/bin/sh
set -x
export PYTHONPATH=''
export BETFAIR_APP_KEY=$1
export BETFAIR_USERNAME=$2
export BETFAIR_PASSWORD=$3

cd /home/ec2-user/bet-edge/oddsscraper/
PATH=$PATH:/usr/local/bin
export PATH
python /home/ec2-user/bet-edge/scripts/new.py
