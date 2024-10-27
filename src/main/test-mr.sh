#!/usr/bin/env bash  
# 表示使用环境变量中的bash解释器来运行该脚本。这样可以确保脚本在不同系统上找到正确的bash路径。

#
# basic map-reduce test
#

#RACE=

# go build -race -buildmode=plugin ../mrapps/wc.go
# go build: go 语言构建命令，用于编译 go 源码文件
# -race: 这个标志启用竞态检测，帮助你检测程序中可能的竞争条件（即多个goroutine同时访问共享数据）。
# -buildmode=plugin: 这个标志指定构建模式为插件，意味着生成的文件可以作为动态库被其他Go程序加载。
# ../mrapps/wc.go: 这是要编译的Go源文件的路径，在这里是相对路径，指向名为wc.go的文件，该文件通常是字数统计的实现。
# 整体来说，这个命令会编译wc.go文件，生成一个可以动态加载的插件，并启用竞态检测功能。

# comment this to run the tests without the Go race detector.
RACE=-race

# mr-tmp 应该是用来存放临时文件的, mr 是 MapReduce 的缩写
# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# 使用 () 括起来的命令用于创建一个子shell。这样做的主要目的包括：
# 隔离环境：在子shell中执行的命令不会影响当前 shell 的环境变量和工作目录。例如，在子shell中修改环境变量或目录不会影响父shell。
# 下面的代码看起来是把 MapReduce apps 挨个编译成 .so 动态库
# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
# CYHNO_TE: 为什么这里的编译不需要加 -buildmode=plugin ?
# 仔细看了一下，下面这三个 go 文件都是有 main 函数的
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

# 这个变量估计是任意一个测试 Failed 了都会被设置为 1，作为全局测试是否通过的判断标准
failed_any=0

#########################################################
# first word-count

# 这一步的目的是为了生成 mr-correct-wc.txt，一个正确的单词数量统计结果
# mrsequential 只有一个map 和 一个reduce，串行执行
# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 180s ../mrcoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait
exit

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s ../mrcoordinator ../pg*txt &
sleep 1

# start multiple workers
timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so &
timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

timeout -k 2s 180s ../mrcoordinator ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so &
timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait


#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

timeout -k 2s 180s ../mrcoordinator ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so &
timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f mr-*

timeout -k 2s 180s ../mrcoordinator ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so &
timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so
timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so &
timeout -k 2s 180s ../mrworker ../../mrapps/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -ne "8" ]
then
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
else
  echo '---' job count test: PASS
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
rm -f mr-*

echo '***' Starting early exit test.

timeout -k 2s 180s ../mrcoordinator ../pg*txt &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/early_exit.so &
timeout -k 2s 180s ../mrworker ../../mrapps/early_exit.so &
timeout -k 2s 180s ../mrworker ../../mrapps/early_exit.so &

# wait for any of the coord or workers to exit
# `jobs` ensures that any completed old processes from other tests
# are not waited upon
jobs &> /dev/null
wait -n

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
(timeout -k 2s 180s ../mrcoordinator ../pg*txt ; touch mr-done ) &
sleep 1

# start multiple workers
timeout -k 2s 180s ../mrworker ../../mrapps/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/824-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
