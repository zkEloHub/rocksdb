#! /bin/bash

SET_BASE_ENV(){
    bench_db_path="/mnt/ssd/ceshi"
    wal_dir=""
    bench_value="4096"                          # sizeof each value
    bench_compression="none" #"snappy,none"     # Algorithm to use to compress the database
    # bench_benchmarks="fillrandom,fillseq,stats,readseq,readrandom,stats"
    bench_benchmarks="fillrandom,stats,readseq,readrandom,stats,fillseq"

    # ------ num, reads
    bench_num="20000000"         # number of k/v to place in database.
    bench_readnum="10000000"     # Number of read operations to do
    #bench_max_open_files="1000"
    max_background_jobs="3"     # The maximum number of concurrent background jobs that can occur in parallel.

    # L1 层的默认大小； 往后每一层 L 层的大小为：
    #   (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))。 xxx_multiplier 默认为 10
    # max_bytes_for_level_base="`expr 8 \* 1024 \* 1024 \* 1024`"     # max bytes for [level-1]
    max_bytes_for_level_base="`expr 256 \* 1024 \* 1024`"

    pmem_path=""
    use_nvm=""

    # 暂不支持 latency
    # report_ops_latency="true"
    # report_write_latency="true"
    # report_fillrandom_latency="true"

    histogram="true"
    threads="1"
}

# SET_BASE_ENV

base_dir=""
data_dir=""

SET_PARAMS(){
    const_params="
        --db=$bench_db_path \
        --wal_dir=$wal_dir \
        --value_size=$bench_value \
        --benchmarks=$bench_benchmarks \
        --num=$bench_num \
        --reads=$bench_readnum \
        --compression_type=$bench_compression \
        --max_background_jobs=$max_background_jobs \
        --max_bytes_for_level_base=$max_bytes_for_level_base \
        "
    
    if [ -n "$use_nvm" ]; then
        const_params=$const_params"--use_nvm_module=$use_nvm "
    fi

    if [ -n "$pmem_path" ]; then
        const_params=$const_params"--pmem_path=$pmem_path "
    fi

    if [ -n "$report_ops_latency" ];then
        const_params=$const_params"--report_ops_latency=$report_ops_latency "
    fi

    if [ -n "$report_fillrandom_latency" ]; then
        const_params=$const_params"--report_fillrandom_latency=$report_fillrandom_latency "
    fi

    if [ -n "$report_write_latency" ]; then
        const_params=$const_params"--report_write_latency=$report_write_latency "
    fi

    if [ -n "$histogram" ]; then
        const_params=$const_params"--histogram=$histogram "
    fi

    if [ -n "$threads" ]; then
        const_params=$const_params"--threads=$threads "
    fi
}

SET_MATRIX_ENV(){
    pmem_path="/mnt/myPMem/nvm"
    use_nvm="true"
    wal_dir="/mnt/ssd/log"
    SET_PARAMS
}

SET_ROCKS_ENV(){
    pmem_path=""
    use_nvm=""
    wal_dir="/mnt/ssd/log"
    SET_PARAMS
}

result_file="out.out"

SET_BENCH_PATH(){
    bench_file_path="$(dirname $PWD )/db_bench"

    if [ ! -f "${bench_file_path}" ];then
    bench_file_path="$PWD/db_bench"
    fi

    if [ ! -f "${bench_file_path}" ];then
    echo "Error:${bench_file_path} or $(dirname $PWD )/db_bench not find!"
    exit 1
    fi

    cmd="$bench_file_path $const_params "
    echo $cmd > $result_file
    echo $cmd
    eval $cmd >> $result_file
}

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

    # echo $cmd
    echo "remove old data..."
    eval $cmd
    sleep 2
    sync
    echo 3 > /proc/sys/vm/drop_caches
    sleep 2
    # echo "clean cache end"
}

# 存储上一次的数据
SAVE_DATA(){
    if [ -d $base_dir/$data_dir ]; then
        eval "rm -r $base_dir/$data_dir"
    fi
    cmd="mkdir $base_dir/$data_dir"
    eval $cmd
    
    # cmd="mv *.csv NVM_LOG OP_DATA out.out $base_dir/$data_dir/"
    cmd="mv out.out $base_dir/$data_dir/"
    # echo "execute cmd: $cmd"
    eval $cmd
    cmd="mv *.csv NVM_LOG OP_DATA $base_dir/$data_dir/"
    eval $cmd

    # 如果出错，记录 core
    if [ -f "core" ]; then
        eval "mv core $base_dir/$data_dir/"
    fi

    sleep 1
    CLEAN_CACHE
}

# 设置/获取时间
time_file="time_file"
now_test=""
GET_START(){
    start=`date +%s`
}

GET_END(){
    end=`date +%s`
    echo "$now_test read: ${num_arr[$i]}, write: ${read_arr[$i]}, cost: `expr $end - $start`s" >> $time_file
}

# 测试 MatrixKV
TEST_MATRIX(){
    SET_MATRIX_ENV
    GET_START
    SET_BENCH_PATH

    data_dir=$1
    now_test=$2

    GET_END
    SAVE_DATA
}

# 测试 RocksDB
TEST_ROCKSDB(){
    SET_ROCKS_ENV
    GET_START
    SET_BENCH_PATH

    data_dir=$1
    now_test=$2

    GET_END
    SAVE_DATA
}

# 测试线程数
TEST_THREAD_NUMBER(){
    SET_BASE_ENV

    base_dir="Test_threads_number"
    if [ ! -d $base_dir ]; then
        mkdir $base_dir
    fi
    eval "rm -r $base_dir/*"

    thread_arr=(1 10 50 100 500 1000)
    for var in ${thread_arr[@]}
    do
        threads="$var"

        TEST_MATRIX "m_threads_$var" "MatrixKV"

        sleep 5

        TEST_ROCKSDB "r_threads_$var" "RocksDB"

    done
}

# TEST_THREAD_NUMBER

# 测试不同数据集
# 读,写: 200w,100w;  500w,250w;  1000w,500w;  5000w,2500w
TEST_DATA_SET(){
    SET_BASE_ENV

    base_dir="Test_db_set"
    if [ ! -d $base_dir ]; then
        eval "mkdir $base_dir"
    fi
    eval "rm -r $base_dir/*"

    # 论文: 2000w左右
    # 8G; 32G; 128G; 256G
    # num_arr=(2000000 8000000 20000000)
    # read_arr=(1000000 4000000 10000000)
    num_arr=(8000000)
    read_arr=(8000000)
    for i in ${!num_arr[@]};
    do
        bench_num="${num_arr[$i]}"
        bench_readnum="${read_arr[$i]}"

        TEST_MATRIX "m_vdataset_`expr ${num_arr[$i]} / 10000`wW_`expr ${read_arr[$i]} / 10000`wR" "MatrixKV"

        sleep 5

        TEST_ROCKSDB "r_vdataset_`expr ${num_arr[$i]} / 10000`wW_`expr ${read_arr[$i]} / 10000`wR" "RocksDB"

    done
}

TEST_DATA_SET

# 测试不同 value_size 大小
TEST_VALUE_SIZE(){
    SET_BASE_ENV

    base_dir="Test_value_size"
    if [ ! -d $base_dir ]; then
        mkdir $base_dir
    fi
    eval "rm -r $base_dir/*"

    # r, w : 20000000

    # value_arr=(256 512 1024 4096 8192 16384)
    value_arr=(256 512 1024)
    for var in ${value_arr[@]}
    do
        bench_value="$var"

        TEST_MATRIX "m_vsize_$var" "MatrixKV"

        sleep 5

        TEST_ROCKSDB "r_vsize_$var" "RocksDB"
    done
}

# TEST_VALUE_SIZE

# 保持总数据量不变，测试不同 value size
TEST_KEEP_DATA(){
    SET_BASE_ENV

    base_dir="Test_80G_data"
    if [ ! -d $base_dir ]; then
        mkdir $base_dir
    fi
    eval "rm -r $base_dir/*"

    value_arr=(256 512 1024 4096 8192 16384)
    num_arr=(320000000 160000000 80000000 20000000 10000000 5000000)
    read_arr=(160000000 80000000 40000000 10000000 5000000 2500000)

    for i in ${!value_arr[@]}
    do
        bench_value="${value_arr[$i]}"
        bench_num="${num_arr[$i]}"
        bench_readnum="${read_arr[$i]}"

        TEST_MATRIX "m_vsize_$bench_value" "MatrixKV"

        sleep 3

        TEST_ROCKSDB "r_vsize_$bench_value" "RocksDB"
        
        sleep 3
    done
}
# TEST_KEEP_DATA
