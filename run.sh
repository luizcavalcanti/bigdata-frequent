#!/bin/bash

# variables
HADOOP_BASE=/usr/local/Cellar/hadoop/1.2.1/libexec
HFS_BASE_DIR=/user/luiz/mushroom
HFS_INPUT_DIR=$HFS_BASE_DIR/input
HFS_OUTPUT_DIR=$HFS_BASE_DIR/output


# deletes previous output directory
$HADOOP_BASE/bin/hadoop dfs -rm $HFS_INPUT_DIR/*
$HADOOP_BASE/bin/hadoop dfs -put data/file* $HFS_INPUT_DIR/

# clean and recopy input content
$HADOOP_BASE/bin/hadoop dfs -rmr $HFS_OUTPUT_DIR*

# runs experiment
$HADOOP_BASE/bin/hadoop jar dist/frequent.jar br.edu.ufam.icomp.MushroomCount $HFS_INPUT_DIR $HFS_OUTPUT_DIR 4 .3 10