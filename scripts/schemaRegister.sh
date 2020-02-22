#!/bin/bash

# usage : ./registerSchame ./ZSPLIT_T01.schema

#export host_s3sync=10.155.97.30
export host_s3sync=10.155.89.236
# schema file which helps to convert csv into parquet
export schema_file=$1

export domain_name=`grep "domain_name" $1 | awk -F : '{print $2}' | awk -F \" '{print $2}'`

# java version should be 1.8+
export JAVA_HOME=/apps/jdk1.8.0_201
export PATH=$JAVA_HOME/bin:$PATH

scp -q ${schema_file} ${host_s3sync}:~/s3_sync/.staging/.${schema_file} &&
ssh ${host_s3sync} "mkdir -p ~/s3_sync/_meta/${domain_name} && mv ~/s3_sync/.staging/.${schema_file} ~/s3_sync/_meta/${domain_name}/_.schema"