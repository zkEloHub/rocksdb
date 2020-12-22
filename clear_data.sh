#! /bin/bash


# 清除上一次测试的数据
CLEAN_CACHE(){

    cmd="rm /mnt/myPMem/ceshi/* /mnt/myPMem/log/* /mnt/myPMem/nvm/* /mnt/ssd/log/* /mnt/ssd/ceshi/* *.csv"

    if [ -f "NVM_LOG" ]; then
        eval "rm NVM_LOG"
    fi

    if [ -f "OP_DATA" ]; then
        eval "rm OP_DATA"
    fi

    if [ -f "out.out" ]; then
        eval "rm out.out"
    fi
    eval $cmd
    sleep 2
    sync
    echo 3 > /proc/sys/vm/drop_caches
    sleep 1
}

CLEAN_CACHE
#! /bin/bash

clean_cacle(){
    cmd = "rm -rf data/"
    eval $cmd
    if [ -f "NVM_LOG" ]; then
        eval "rm NVM_LOG"
    fi

    cmd = "rm -rf /mnt/myPMem/*"
    eval $cmd
}