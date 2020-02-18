package com.se.edw

import java.io.{File, PrintWriter}
import java.util
import java.util.UUID

import com.se.edw.model.deltalog
import com.se.edw.model.deltalog.{DeltaLogEditor, DeltaLogItem}
import com.se.edw.model.mergelog
import com.se.edw.model.mergelog.{MergeLogEditor, MergeLogItem}
import com.se.edw.model.domain.DomainObjectSchema
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory
import com.se.edw.env.{EnvEDW, _}

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConversions.asJavaCollection

object ModelInitialize extends EnvEDW with EnvAWS{
    def main(args: Array[String]): Unit = {

//        val bucketName = "edw-core-dev"
//        val srcKeys = List("_meta/TEST/BW/_BIC_AZAPD_D1600/_.schema")
        val bucketName = args(0)
        val srcKeys = args.tail
        s3Util.setBucket(bucketName)
        val defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"
        val archiveBucketName =
            s3Client.listBuckets().map(b => b.getName).find(b => b.equals(s3Util.getBucket + "-archive"))
                .getOrElse(s3Util.getBucket)


        srcKeys.foreach( srcKey => {
            val schemaObjecct = DomainObjectSchema(s3Util.getBucket,srcKey)

            //if archive Bucket is the same as the operation bucket .
            //there should be a _archive area .
            val archivePrefix =
                if (archiveBucketName.equals(bucketName))
                    Domain_Area.Archive_area + "/"
                else
                    ""


            //temporary local file
            val tmpCsv = new File("/tmp/" + UUID.randomUUID().toString() + ".csv")
            val tmpParquet = new File("/tmp/" + UUID.randomUUID().toString() + ".parquet")
            val tmpSchema = new File("/tmp/" + UUID.randomUUID().toString() + ".schema")

            //make schema file
            jsonMapper.writeValue(tmpSchema, schemaObjecct.schema)
            //make csv file with only header but no data
            val writer = new PrintWriter(tmpCsv)
            writer.write(schemaObjecct.fields.map(f => f.fieldName).mkString(","))
            writer.close()
            //make parquet file with the empty csv file and schema file
            val console = LoggerFactory.getLogger(this.getClass)
            val t = new ConvertCSVCommand(console)
            t.setConf(new Configuration)
            val targets = new util.ArrayList[String]
            targets.add(tmpCsv.getPath)
            t.setTargets(targets)
            t.setAvroSchemaFile(tmpSchema.getPath)
            t.setOutputPath(tmpParquet.getPath)
            t.setOverwrite(true)
            t.run

            val partitions:List[String] = schemaObjecct.model.snapshot.getOrElse("partitionBy",List[String]())
              .asInstanceOf[List[String]]
            val partitionsPath:String = partitions.map(p => s"$p=$defaultPartitionName").mkString("/")

            //put 0.parquet into delta model
            s3Client.putObject(s3Util.getBucket,
                schemaObjecct.shiftToArea(Domain_Area.Model_area,Domain_Model.Delta,"0.parquet"),
                tmpParquet)

            //put 0.parquet into snapshot model
            s3Client.putObject(s3Util.getBucket,
                schemaObjecct.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot,partitionsPath,"0.parquet"),
                tmpParquet)


            //make _delta_log/00000000000000000000.json for delta model
            val deltaInitLogForDelta = {
                val commitInfo = deltalog.CommitInfo(operationParameters_mode = DeltaLogEditor.operationMode.ErrorIfExists)
                val protocolInfo = deltalog.ProtocalInfo()
                val metaDataInfo = deltalog.MetaDataInfo(jsonMapper.createObjectNode()
                    .put("type","struct")
                    .set("fields",jsonMapper.createArrayNode().addAll(
                        schemaObjecct.fields.map(f => {
                            jsonMapper.createObjectNode()
                                .put("name",f.fieldName)
                                .put("type",f.filedType)
                                .put("nullable",f.nullable)
                                .set("metadata",jsonMapper.createObjectNode())
                        })
                    )).toString
                )
                val addInfo = deltalog.AddInfo("0.parquet",tmpParquet.length())
                DeltaLogItem(commitInfo,Some(protocolInfo),Some(metaDataInfo),Some(List(addInfo)),None)
            }
            //make _delta_log/00000000000000000000.json for snapshot model
            val deltaInitLogForSnapshot = {
                val commitInfo = deltalog.CommitInfo(
                    operationParameters_mode = DeltaLogEditor.operationMode.ErrorIfExists,
                    operationParameters_partitionBy = "[" + {
                        if (partitions.size == 0)
                            ""
                        else
                            "\"" + partitions.mkString("\",\"") + "\""
                    }+ "]"
                )
                val protocolInfo = deltalog.ProtocalInfo()
                val metaDataInfo = deltalog.MetaDataInfo(schemaString = jsonMapper.createObjectNode()
                    .put("type","struct")
                    .set("fields",jsonMapper.createArrayNode().addAll(
                        schemaObjecct.fields.map(f => {
                            jsonMapper.createObjectNode()
                                .put("name",f.fieldName)
                                .put("type",f.filedType)
                                .put("nullable",f.nullable)
                                .set("metadata",jsonMapper.createObjectNode())
                        })
                    )).toString,
                    partitionColumns = {
                        val an = jsonMapper.createArrayNode()
                        partitions.foreach(an.add(_))
                        an
                    }
                )
                val addInfo = deltalog.AddInfo(
                    path = partitions.map(p => s"$p=$defaultPartitionName").:+("0.parquet").mkString("/"),tmpParquet.length(),
                    partitionValues = partitions.foldLeft(jsonMapper.createObjectNode()){(onode,ptn) => onode.putNull(ptn)}
                )
                DeltaLogItem(commitInfo,Some(protocolInfo),Some(metaDataInfo),Some(List(addInfo)),None)
            }
            //put _delta_log/00000000000000000000.json into delta model
            DeltaLogEditor(schemaObjecct.shiftToArea(Domain_Area.Model_area,Domain_Model.Delta))
                .putLog(0,deltaInitLogForDelta )

            //put _delta_log/00000000000000000000.json into snapshot model
            DeltaLogEditor(schemaObjecct.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot))
                .putLog(0,deltaInitLogForSnapshot )

            //make _merge_log/00000000000000000000.json
            val mergeInitLog = {
                val commitInfo = mergelog.CommitInfo(mode = MergeLogEditor.operationMode.ErrorIfExists)
                val addInfo = mergelog.HandleInfo(0,"0.parquet",tmpParquet.length())
                MergeLogItem(commitInfo,List(addInfo))
            }
            //put _merge_log/00000000000000000000.json into snapshot model
            MergeLogEditor(schemaObjecct.shiftToArea(Domain_Area.Model_area,Domain_Model.Snapshot))
                .putLog(0,mergeInitLog)

            //Copy initialized model into archive area for rebuild
            s3Util.s3ObjectSummariesWithNoTruncate(s3Util.getBucket,schemaObjecct.shiftToArea(Domain_Area.Model_area)).foreach( obj => {
                s3Client.copyObject(
                    s3Util.getBucket,
                    obj.getKey,
                    archiveBucketName,
                    archivePrefix + obj.getKey
                )
            })


            tmpCsv.delete()
            tmpSchema.delete()
            tmpParquet.delete()
        })

    }
}
