package com.se.edw.service.modelTransformer

import com.se.edw.model.deltalog
import com.se.edw.model.deltalog.{AddInfo, DeltaLogEditor, DeltaLogItem, RemoveInfo}
import com.se.edw.model.domain.DomainObjectSchema
import com.se.edw.model.mergelog.MergeLogEditor.operationMode
import com.se.edw.model.mergelog.{CommitInfo, HandleInfo, MergeLogEditor, MergeLogItem}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

object SnapshotMakerByAppend extends ModelMaker {

    def make(schemaObject:DomainObjectSchema, options:Map[String,String])(implicit spark:SparkSession):Unit = {
        val snapshotDate:String = options.get("snapDate").get
        val batch:Int = options.get("batch").foldLeft(0){ (_,value) => Integer.valueOf(value)}
        //kinds of Log Editors
        val snapshotMergeLogEditor =
            MergeLogEditor(schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot))
        val snapshotDeltaLogEditor =
            DeltaLogEditor(schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot))
        val deltaDeltaLogEditor =
            DeltaLogEditor(schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Delta))
        //get last snapshot information
        val mergeLogItem = snapshotMergeLogEditor.getLogByVersion(snapshotMergeLogEditor.lastVersion)
        val lastSnapshotDate = mergeLogItem.commitInfo.snapshotDate.split("-")
        val lastSnapshotPath = schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot,
            "_year=" + lastSnapshotDate(0),
            "_month=" + StringUtils.leftPad(lastSnapshotDate(1),2,"0") ,
            "_day=" + StringUtils.leftPad(lastSnapshotDate(2),2,"0") ,
            "_ts=" + mergeLogItem.commitInfo.timestamp)
        //get new snapshot information
        val newTimestamp = calendar.getTimeInMillis
        val newSnapshotDate = snapshotDate.split("-")
        val newSnapshotPath = schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot,
            "_year=" + newSnapshotDate(0),
            "_month=" + StringUtils.leftPad(newSnapshotDate(1),2,"0"),
            "_day=" + StringUtils.leftPad(lastSnapshotDate(2),2,"0") ,
            "_ts=" + newTimestamp)
        //find the last batch of deltas which has beed appended into last snapshot, and get the last one
        val mergeLogItemContent = mergeLogItem.getContent
        val previousDeltaNo = mergeLogItemContent .split("\\n")
            .map(line => {
                val lineObject = jsonMapper.readTree(line)
                if (lineObject.has("add")){
                    new HandleInfo(lineObject).deltaVersion
                }else{
                    0
                }
            }).max
        //get the last one delta in Delta model
        val lastDeltaNo = deltaDeltaLogEditor.lastVersion
        //if there is new delta occurring
        if (lastDeltaNo > previousDeltaNo ) {

            //copy the last snapshot into new one
            s3Util.s3ObjectSummariesWithNoTruncate(schemaObject.bucketLocation,lastSnapshotPath)
                .foreach(obj => {
                    s3Client.copyObject(
                        obj.getBucketName,
                        obj.getKey,
                        obj.getBucketName,
                        newSnapshotPath + "/" + obj.getKey.split("/").last
                    )
            })
            //add the deltas(previousNo + 1 -> lastNo) into new snapshot,
            // record the transaction log info
            var loop = true
            val newDeltaInfos = (for(v <- previousDeltaNo + 1 to lastDeltaNo if loop) yield {
                //if batch > 0 ,then stop the loop after "batch" times merging .
                if (v - previousDeltaNo == batch )
                    loop = false
                //append new delta into snapshot
                deltaDeltaLogEditor.getLogByVersion(v).addInfos.get
                    .filter(obj => ! obj.path.equals("0.parquet") )
                    .map(delta => {
                        val newPath = newSnapshotPath + "/" + delta.path.split("/").last
                        s3Client.copyObject(
                            schemaObject.bucketLocation,
                            schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Delta,delta.path),
                            schemaObject.bucketLocation,
                            schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot,newPath)
                        )
                        Tuple2(
                            HandleInfo(v,delta.path,delta.size),
                            AddInfo(newPath,delta.size)
                        )
                    })
                }).flatten.toList
            //add merge log
            snapshotMergeLogEditor.putLog(snapshotMergeLogEditor.lastVersion + 1,
                MergeLogItem(
                    CommitInfo(timestamp=newTimestamp,snapshotDate=snapshotDate,mode=operationMode.Append),
                    newDeltaInfos.map(_._1)
                )
            )
            //add delta log
            val lastDeltaLog = snapshotDeltaLogEditor.getLogByVersion(snapshotDeltaLogEditor.lastVersion)
            snapshotDeltaLogEditor.putLog(snapshotDeltaLogEditor.lastVersion + 1,
                DeltaLogItem(
                    deltalog.CommitInfo(snapshotDeltaLogEditor.lastVersion,
                        operationParameters_mode = DeltaLogEditor.operationMode.Append),
                    None,
                    None,
                    Some(newDeltaInfos.map(_._2)),
                    Some(lastDeltaLog.addInfos.get.map(a => RemoveInfo(a.path)))
                )
            )

            if ((snapshotDeltaLogEditor.lastVersion + 1) % 10 == 0)
                DeltaLog.forTable(
                    spark,
                    schemaObject.shiftToAreaWithFullPath(Domain_Area.Model_area,Domain_Model.Snapshot)
                ).checkpoint()

            // Archive the trace of snapshot for re-build .
            val mergeLogPath = schemaObject.shiftToArea(
                Domain_Area.Model_area, Domain_Model.Snapshot,
                "_merge_log", snapshotMergeLogEditor.getLogName(snapshotMergeLogEditor.lastVersion + 1)
            )
            val archivePrefix =
                if(schemaObject.archiveBucketLocation.equals(schemaObject.bucketLocation)) Domain_Area.Archive_area else ""

            if (s3Util.validateObjectExistX3(schemaObject.bucketLocation,mergeLogPath)){
                s3Client.copyObject(
                    schemaObject.bucketLocation,
                    mergeLogPath,
                    schemaObject.archiveBucketLocation,
                    archivePrefix + mergeLogPath
                )
            }
        }
    }
}
