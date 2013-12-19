#!/bin/bash

#### variáveis de ambiente ####
HADOOP_BASE=/usr/local/Cellar/hadoop/1.2.1/libexec
HFS_BASE_DIR=/user/luiz/mushroom
HFS_INPUT_DIR=$HFS_BASE_DIR/input
HFS_OUTPUT_DIR=$HFS_BASE_DIR/output

#### variáveis do experimento ####
NUMERO_PASSOS=4 # número de passos a serem executados (define o tamanho de k)
LIMIAR_SUPORTE=0.70 # percentual de suporte para frequência
TAMANHO_DADOS=0 # numero total de registros, será atribuído depois da conversão de dados

# converte dados para formato mais legível/rastreável
java -classpath dist/frequent.jar br.edu.ufam.icomp.conversor.ConversorDeDados data/agaricus-lepiota.data data/file01

# redefine número total de registros pós-conversão
TAMANHO_DADOS=$(cat data/file01 | wc -l)

# limpa e copia novamente conteúdo do diretório de entrada no HDFS
$HADOOP_BASE/bin/hadoop dfs -rm $HFS_INPUT_DIR/*
$HADOOP_BASE/bin/hadoop dfs -put data/file* $HFS_INPUT_DIR/

# remove todos os diretórios de saída do HDFS
$HADOOP_BASE/bin/hadoop dfs -rmr $HFS_OUTPUT_DIR*

# executa experimento
$HADOOP_BASE/bin/hadoop jar dist/frequent.jar br.edu.ufam.icomp.MushroomCount $HFS_INPUT_DIR $HFS_OUTPUT_DIR $NUMERO_PASSOS $LIMIAR_SUPORTE $TAMANHO_DADOS