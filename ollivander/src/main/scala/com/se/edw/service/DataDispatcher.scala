package com.se.edw.service


import java.util.Calendar

import com.amazonaws.services.s3.model.S3ObjectSummary
import com.se.edw.exception.AWSOperationException
import com.se.edw.model.deltalog._
import com.se.edw.model.domain.DomainObjectSchema
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

object DataDispatcher extends SparkApp {

    var etlDatePath = ""

    override def init(serviceInvoke: ServiceInvoke)(implicit spark: SparkSession): Unit = {
        super.init(serviceInvoke)

        etlDatePath =  "_year=" + calendar.get(Calendar.YEAR) + "/" +
            "_month=" + StringUtils.leftPad((calendar.get(Calendar.MONTH) + 1).toString,2,"0")  + "/" +
            "_day=" + StringUtils.leftPad(calendar.get(Calendar.DAY_OF_MONTH).toString,2,"0")
    }


    override def handle(serviceInvoke: ServiceInvoke)(implicit schemaObject: DomainObjectSchema, spark: SparkSession): Unit = {
        val dispatch = s3Util.s3ObjectSummariesWithNoTruncate(
            schemaObject.bucketLocation,
            schemaObject.shiftToArea(Domain_Area.Input_area)
        ).sortBy(_.getLastModified)
            .map(x =>{
                dispatchAndArchive(x)
            })

        //make a checkpoint every 10(or more than 10) dispatches .
        if ( dispatch.size > 0 && ( dispatch.size >= 10 || dispatch.last % 10 == 0 ) )
            DeltaLog.forTable(
                spark,
                schemaObject.shiftToAreaWithFullPath(Domain_Area.Model_area,schemaObject.mainModelType)
            ).checkpoint()
    }

    def dispatchAndArchive(s3ObjectSummary:S3ObjectSummary)
                       (implicit schemaObject: DomainObjectSchema, spark: SparkSession):Int = {

        val archivePrefix = if(schemaObject.archiveBucketLocation.equals(schemaObject.bucketLocation))
            Domain_Area.Archive_area
        else
            ""
        val pathUnderModel = etlDatePath + "/" + s3ObjectSummary.getKey.split("/").last

        if (s3Client.doesObjectExist(s3Util.getBucket,schemaObject.shiftToArea(Domain_Area.Model_area,schemaObject.mainModelType,pathUnderModel)))
            throw new AWSOperationException(s"The file has already been in the path : ${schemaObject.mainModelType}/${pathUnderModel} , You can't do the dispatch operation")

        val dataPath = schemaObject.shiftToArea(Domain_Area.Model_area,schemaObject.mainModelType, pathUnderModel)

        //copy input data into model area
        s3Client.copyObject(
            s3ObjectSummary.getBucketName,
            s3ObjectSummary.getKey,
            s3ObjectSummary.getBucketName,
            dataPath
        )

        //making transaction log
        val deltaLogEditor = DeltaLogEditor(schemaObject.shiftToArea(Domain_Area.Model_area,schemaObject.mainModelType))
        val lastDeltaLogVersion = deltaLogEditor.lastVersion
        val lastDeltaLog = deltaLogEditor.getLogByVersion(lastDeltaLogVersion)

        deltaLogEditor.putLog(lastDeltaLogVersion + 1,DeltaLogItem(
            CommitInfo(lastDeltaLogVersion,calendar.getTimeInMillis),
            None,
            None,
            Some(List(AddInfo(pathUnderModel,s3ObjectSummary.getSize))),
            Some(lastDeltaLog.addInfos.get.map(a => RemoveInfo(a.path)))
        ))

        //copy input data into archive area from model area
        if (s3Util.validateObjectExistX3(schemaObject.bucketLocation,dataPath))
            s3Client.copyObject(
                schemaObject.bucketLocation,
                dataPath,
                schemaObject.archiveBucketLocation,
                archivePrefix + dataPath
            )

        //copy transaction log into archive area from model area
        val logPath = schemaObject.shiftToArea(Domain_Area.Model_area,schemaObject.mainModelType,
            "_delta_log",deltaLogEditor.getLogName(lastDeltaLogVersion + 1)
        )
        if (s3Util.validateObjectExistX3(schemaObject.bucketLocation,logPath))
            s3Client.copyObject(
                schemaObject.bucketLocation,
                logPath,
                schemaObject.archiveBucketLocation,
                archivePrefix + logPath
            )

        //delete input data after archive successfully .
        if (s3Util.validateObjectExistX3(schemaObject.archiveBucketLocation ,archivePrefix + dataPath) &&
            s3Util.validateObjectExistX3(schemaObject.archiveBucketLocation , archivePrefix + logPath)
        ){
            s3Client.deleteObject(
                s3ObjectSummary.getBucketName,
                s3ObjectSummary.getKey
            )
        }

        lastDeltaLogVersion + 1
    }


}
