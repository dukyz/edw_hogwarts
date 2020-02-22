#!/bin/bash

# usage : cat ZSPLIT_T01.csv | ./sendToS3Directly ./ZSPLIT_T01.schema 'entity=wuxi,period=201908'
# pay attention !! no specific character !!
export bucket=cnn1-s3-inteldatastore-poc
export bucket1=cnn-s3-rg-edw
export http_proxy=http://10.155.97.30:3872
export https_proxy=http://10.155.97.30:3872
# tmp file which will be deleted finally
export tmp_file=`uuidgen`.parquet
export tag=`echo ${2} | tr -d ' ' | tr -d '/' | tr -d '.' | tr -d '@' | tr -d '_'`
# 2019-09-13_00:55:01.365#entity=wuxi,period=201908.parquet
export file_name=`date +'%F_%T.%3N'`"@${tag}.parquet"
# schema file which helps to convert csv into parquet
export schema_file=${1}
export domain_name=`grep "domain_name" ${schema_file} | awk -F : '{print $2}' | awk -F \" '{print $2}'`

# java version should be 1.8+
export JAVA_HOME=/apps/jdk1.8.0_201
export PATH=$JAVA_HOME/bin:$PATH

export delimiter=","
export quote="\""

java -cp '/apps/parquet/lib/*' org.apache.parquet.cli.Main convert-csv stdin -o ${tmp_file} -s ${schema_file} --delimiter ${delimiter} --quote ${quote} --overwrite &&
aws s3 cp ${tmp_file} s3://${bucket}/_input/${domain_name}/${file_name} 

if [[ `echo ${domain_name} | grep 'FI/MDC/'` != "" ]]
then
    aws s3 cp ${tmp_file} s3://${bucket1}/_input/${domain_name}/${file_name}
fi
rm -f ${tmp_file} .${tmp_file}.crc
