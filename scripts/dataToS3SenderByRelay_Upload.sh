#!/bin/bash
export interval=3
export bucketName=edw-core-dev

while true
do
cd /home/s3sync/s3_sync/ && find _input -name *.parquet | xargs -i -r aws s3 cp {} s3://${bucketName}/{} --no-progress |  awk '{"date +%F\" \"%T" | getline t ; print t,$0}' | tee -a .log/data_upload.log | awk '{print $4}' | xargs -i -r rm {} -f
sleep ${interval}
done
