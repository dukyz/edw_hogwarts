#!/bin/bash
# usage : ./signalSuccessSender.sh D1600 201708,201709
export remote_host=10.155.89.236
#export remote_host=10.155.97.30
export remote_user=s3sync
export remote_folder=/apps/s3_sync/_signal
export ts=`date +'%F_%T.%3N'`
export signame=`echo ${1} | tr -d '@' | tr -d '.' | tr -d '/' `
export options=`echo ${2} | tr -d '"'`
export source=BW
export status=START

cat << EOF | ssh ${remote_user}@${remote_host} "cat > ${remote_folder}/${ts}@${signame}.sig"
{
    "success":true,
    "source":"${source}",
    "status":"${status}",
    "signame":"${signame}",
    "options":"${options}"
    "ts":"${ts}"
}
EOF
