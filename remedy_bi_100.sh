#!/bin/bash
#===============================================================================
#
#          FILE: remedy_bi_100.sh
# 
#         USAGE: ./remedy_bi_100.sh 
# 
#   DESCRIPTION: 
# 
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Liu Yinliang
#  ORGANIZATION: 
#       CREATED: 01/09/2017 13:54
#      REVISION:  ---
#===============================================================================
S_DATE=`date -d"1 day ago" +%Y%m%d`

if [ -e 1.txt ]
then
    size=`du -s 1.txt | awk '{print $1}'`
    if [ $size -gt 1 ]
    then
        if [ -e 2.txt ] && [ -e 3.txt ]
        then
            if [ -e done1 ] && [ -e done2 ]
            then
                echo "all done, no need to remedy!"
            else
                echo "alltxts are prepared, now push to leaf(1/2)"
                python leaf.py 2.txt 1 38 ff63a9a52b921aa04827dbf544633bb5 $S_DATE
                echo "python 1/2 done!">done1
                echo "alltxts are prepared, now push to leaf(2/2)"
                python leaf.py 3.txt 1 38 ff63a9a52b921aa04827dbf544633bb5 $S_DATE
                echo "python 2/2 done!">done2
            fi
        else
            echo "SQL done, now R"
            R CMD BATCH 1.R
            size=`du -s 2.txt | awk '{print $1}'`
            if [ $size -gt 0 ]
            then
                echo "now push to leaf(1/2)"
                python leaf.py 2.txt 1 38 ff63a9a52b921aa04827dbf544633bb5 $S_DATE
                echo "python 1/2 done!">done1
            else
                echo "R not done!"
            fi
            size=`du -s 3.txt | awk '{print $1}'`
            if [ $size -gt 0 ]
            then
                echo "now push to leaf(2/2)"
                python leaf.py 3.txt 1 38 ff63a9a52b921aa04827dbf544633bb5 $S_DATE
                echo "python 2/2 done!">done2
            else
                echo "R not done!"
            fi
        fi
    else
        echo "1.txt is null, now run bi_100"
        sh bi_100.sh >log 2>&1
    fi
else
    echo "1.txt is not exit, now run bi_100"
    sh bi_100.sh >log 2>&1
fi

