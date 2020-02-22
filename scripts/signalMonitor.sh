#!/bin/bash
# usage : usermod -aG s3sync airflow; su airflow ; nohup signalMonitor.sh &
export interval=5
export monitor_folder=/apps/s3_sync/_signal
# mapping the relationship between signal and dag
declare -A map_sig_dag=(["D1600"]="run_spark_submit_1600_cogs" ["D1700"]="run_spark_submit_1600_cogs" ["D1800"]="run_spark_submit_1600_cogs")

source /apps/env/python3.7.4/bin/virtualenvwrapper.sh
workon airflow
while true 
do
    # find the signal file and trigger the dag
    for sig_file in `find $monitor_folder -maxdepth 1 -name '*.sig' | awk -F / '{print $NF}' | sort`
    do
        export sig_name=`echo ${sig_file} | awk -F @ '{print $2}' | tr -d '.sig'`
        export dag_id=${map_sig_dag[${sig_name}]}
        # no mapping signal will be moved to the default folder @
        if [ -z "${dag_id}" ];then
        	export dag_id=@
        fi
        mkdir ${monitor_folder}/${dag_id} -p
        # trigger the dag ,log into sig_file.log
        if [ "${dag_id}" != "@" ];then
            export options=`grep options ${monitor_folder}/${sig_file} | awk -F : '{print $2}' |tr -d '"'`
            airflow trigger_dag -c "{\"options\":\"${options}\"}" ${dag_id} > ${monitor_folder}/${dag_id}/${sig_file}.log
        fi
        # move signal file into the corresponding folder
        mv ${monitor_folder}/${sig_file} ${monitor_folder}/${dag_id}/
    done
    sleep ${interval}
done
