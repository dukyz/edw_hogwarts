package com.se.edw.service.modelTransformer

import com.se.edw.exception.DomainObjectSchemaException
import com.se.edw.model.deltalog.DeltaLogEditor
import com.se.edw.model.domain.DomainObjectSchema
import com.se.edw.model.mergelog.MergeLogEditor.operationMode
import com.se.edw.model.mergelog.{CommitInfo, HandleInfo, MergeLogEditor, MergeLogItem}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, SparkSession}


object SnapshotMakerByMerge extends ModelMaker {

    def make(schemaObject:DomainObjectSchema, options:Map[String,String])(implicit spark:SparkSession):Unit = {
        //date of snapshot making
        val snapshotDate:String = options.get("snapDate").get
        val batch:Int = options.get("batch").foldLeft(0){ (_,value) => Integer.valueOf(value)}
        //Merge Log Editors in snapshot model
        val snapshotMergeLogEditor =
            MergeLogEditor(schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot))
        //Delta Log Editors in delta model
        val deltaDeltaLogEditor =
            DeltaLogEditor(schemaObject.shiftToArea(Domain_Area.Model_area,Domain_Model.Delta))


        //find the last batch of deltas which has beed merged into last snapshot,
        // and get the last one as the last one which has been handled . So called "previousDeltaNo"
        val mergeLogItemContent = snapshotMergeLogEditor.getLogByVersion(snapshotMergeLogEditor.lastVersion).getContent
        val previousDeltaNo = mergeLogItemContent .split("\\n")
            .map(line => {
                val lineObject = jsonMapper.readTree(line)
                if (lineObject.has("add")){
                    new HandleInfo(lineObject).deltaVersion
                }else{
                    0
                }
            }).max
        //get the last one delta in Delta model , which maybe has not been handled yet. So called "lastDeltaNo"
        val lastDeltaNo = deltaDeltaLogEditor.lastVersion

        //if there is new delta occurring , Do the merge !
        if (lastDeltaNo > previousDeltaNo ) {

            //The basic condition of merge is the PK equality
            val pks = schemaObject.pks
            val pksCondition = pks.map( pk => s" master.$pk = change.$pk ").mkString(" and ")

            var loop = true
            for (v <- previousDeltaNo + 1 to lastDeltaNo if loop ) yield {
                //if batch > 0 ,then stop the loop after "batch" times merging .
                if (v - previousDeltaNo == batch )
                    loop = false
                //get the partitionBy from schema info
                val O_partitionKeys = schemaObject.model.snapshot.get("partitionBy").asInstanceOf[Option[List[String]]]
                //get the replaceBy from schema info
                val O_replaceKeys = schemaObject.model.snapshot.get("replaceBy").asInstanceOf[Option[List[String]]]

                if(O_replaceKeys.isDefined && O_partitionKeys.isEmpty) {
                    throw new DomainObjectSchemaException("replaceKeys should not exist without partitionKeys's existing ！")
                }
                if(O_replaceKeys.isDefined && O_partitionKeys.isDefined
                        && !O_partitionKeys.get.containsSlice(O_replaceKeys.get) ) {
                    throw new DomainObjectSchemaException("replaceKeys must be contained in partitionKeys ！")
                }

                //get the dataFrame of delta file and repartition if there is partition
                val change = O_partitionKeys.foldLeft(
                    spark.read.format("delta").option("versionAsOf", v)
                        .load(schemaObject.shiftToAreaWithFullPath(Domain_Area.Model_area,Domain_Model.Delta))
                ){ (df , partitionKeys) =>
                    df.repartition(partitionKeys.map(new Column(_)):_*)
                }

                null match {
                    //if replaceKey was defined . Then the partitionKeys must have been defined first .
                    // So the replaceWhere strategy could be used .
                    //This strategy could tolerate the duplication in data . As it is a kind of replacement locally.
                    case _ if (O_replaceKeys.isDefined && O_replaceKeys.get.size > 0) => {
                        //find the replace values in change file and form the replace sub-condition and use replace strategy .
                        val replaceKeysValue = change.select(O_replaceKeys.get.map(new Column(_)):_*).distinct().collect()
                        val replaceCondition = "(" +
                            replaceKeysValue.map(row  => {
                                O_replaceKeys.get.map(key => s"${key} = " + "\"" + row.getAs[String](key) + "\"").mkString(" and ")
                            }).mkString(" or ") +
                            ")"
                        change.write.format("delta")
                            .mode("overwrite")
                            .partitionBy(O_partitionKeys.get:_*)
                            .option("replaceWhere", replaceCondition)
                            .save(schemaObject.shiftToAreaWithFullPath(Domain_Area.Model_area,Domain_Model.Snapshot))
                    }
                    //if replaceKey was not defined .But only partitionKeys was defined .
                    // Then partition merge strategy could be used .
                    //This strategy can't tolerate the duplication in data . As it is a kind of merging by PKs
                    case _ if (O_partitionKeys.isDefined && O_partitionKeys.get.size > 0) => {
                        //find the partition values in change file and form the partition sub-condition
                        val partitionKeysValue = change.select(O_partitionKeys.get.map(new Column(_)):_*).distinct().collect()
                        val partitionCondition = "(" +
                            partitionKeysValue.map(row  => {
                                O_partitionKeys.get.map(key => s"master.${key} = " + "\"" + row.getAs[String](key) + "\"").mkString(" and ")
                            }).mkString(" or ") +
                            ")"
                        //if the change data set is empty , so there will be no #partitionCondition# , And there will be no need to do the match
                        //but the "snapshot making" will go on ,So the partitionCondition will be replaced with "1=0" , And there will be no result matched at all .
                        val partitionCondition_withEmpty = if ("()".equals(partitionCondition)) "1=0" else partitionCondition
                        DeltaTable
                            .forPath(spark,schemaObject.shiftToAreaWithFullPath(Domain_Area.Model_area,Domain_Model.Snapshot))
                            .as("master")
                            .merge(change.as("change"), pksCondition + " and " + partitionCondition_withEmpty )
                            .whenMatched( partitionCondition_withEmpty )
                            .updateAll()
                            .whenNotMatched()
                            .insertAll()
                            .execute()
                    }
                    //if neither of partitionKeys and replaceKeys were defined .
                    //Then the basic pks merge strategy could be used.
                    //This strategy can't tolerate the duplication in data .As it is merging by PKs
                    case _ => {
                        DeltaTable
                            .forPath(spark,schemaObject.shiftToAreaWithFullPath(Domain_Area.Model_area,Domain_Model.Snapshot))
                            .as("master")
                            .merge(change.as("change"), pksCondition )
                            .whenMatched().updateAll()
                            .whenNotMatched().insertAll()
                            .execute()
                    }
                }

                val handledInfos = deltaDeltaLogEditor.getLogByVersion(v).addInfos.get.map(delta => {
                    HandleInfo(v,delta.path,delta.size)
                })
                //add merge log
                snapshotMergeLogEditor.putLog(v,MergeLogItem(
                        CommitInfo(snapshotDate=snapshotDate,mode=operationMode.Merge),
                        handledInfos
                    )
                )
                // Archive the trace of snapshot for re-build .
                val mergeLogPath = schemaObject.shiftToArea(
                    Domain_Area.Model_area, Domain_Model.Snapshot,
                    "_merge_log", snapshotMergeLogEditor.getLogName(snapshotMergeLogEditor.lastVersion + v - previousDeltaNo)
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
}