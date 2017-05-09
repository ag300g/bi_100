#!/bin/bash 
#===============================================================================
#
#          FILE: reset_bi_100.sh
# 
#         USAGE: ./reset_bi_100.sh 
# 
#   DESCRIPTION: reset the file, prepare for the next run
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: ag300g 
#  ORGANIZATION: 
#       CREATED: 01/08/2017 14:21
#      REVISION:  ---
#===============================================================================
if [ -e log ]
    then
    rm log
fi

if [ -e 1.txt ]
    then 
    rm 1.txt 
fi

if [ -e 2.txt ]
then
    rm 2.txt
    rm 1.Rout
fi

if [ -e 3.txt ]
then
    rm 3.txt
fi

if [ -e done1 ]
then
    rm done1
fi

if [ -e done2 ]
then
    rm done2
fi

for (( t=1; t<=7; t++ ))
do
    if [ -e remedy_log${t} ]
        then
        rm remedy_log${t}
    fi
done
echo "reset done, prepare to the next run."

