#!/bin/bash
export interval=3
export bucketName=edw-core-dev
export modelInitializer=/home/s3sync/s3_sync/.lib/model-initializer-assembly-0.1.jar

while true
do
cd /home/s3sync/s3_sync/ && find _meta -name _.schema | xargs -i -r aws s3 cp {} s3://${bucketName}/{} --no-progress |  awk '{"date +%F\" \"%T" | getline t ; print t,$0}' | tee -a .log/schema_upload.log | awk '{print $4}' | while read f ; do rm ${f} -f  && java -jar ${modelInitializer} ${bucketName} ${f} ; done ; 
sleep ${interval}
done