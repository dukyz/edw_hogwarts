package com.se.edw.service

import com.se.edw.env._
import com.se.edw.model.domain.DomainObjectSchema
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions.collectionAsScalaIterable


object ModelRebuilder extends SparkApp {


    var archiveBucketName:String = ""

    override def init(serviceInvoke: ServiceInvoke)(implicit spark: SparkSession): Unit = {
        super.init(serviceInvoke)

        archiveBucketName =
            s3Client.listBuckets().map(b => b.getName).find(b => b.equals(s3Util.getBucket + "-archive"))
                .getOrElse(s3Util.getBucket)
    }

    override def handle(serviceInvoke: ServiceInvoke)(implicit schemaObject: DomainObjectSchema, spark: SparkSession): Unit = {


        val archiveList = s3Util.s3ObjectSummariesWithNoTruncate(
            archiveBucketName,
            schemaObject.shiftToArea(Domain_Area.Archive_area)
        ).sortBy(_.getLastModified)


        val rebuiltList = s3Util.s3ObjectSummariesWithNoTruncate(
            s3Util.getBucket,
            schemaObject.shiftToArea(Domain_Area.Archive_area)
        )

        DataDispatcher.handle(serviceInvoke)
        ModelTransformer.handle(serviceInvoke)
    }
}








