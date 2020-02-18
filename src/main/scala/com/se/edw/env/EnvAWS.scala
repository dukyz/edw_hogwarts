package com.se.edw.env

import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy
import com.amazonaws.services.s3.model.{ListObjectsV2Request, S3ObjectSummary}
import com.se.edw.exception.AWSOperationException

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.mutable.MutableList


trait EnvAWS extends EnvBasic {

    val s3Util = EnvAWS.s3Util
    val s3Client:AmazonS3 = s3Util.s3Client

}

object EnvAWS {
    /**
      * @TODO: utils of AWS S3
      */
    object s3Util {

        private var _bucketName:String = ""
        private var _archiveBucketName:String = ""

        def setBucket(bucketName:String) :s3Util.type = {
            if (!"".equals(_bucketName)) {
                throw new Exception("Bucket has been set already ! ")
            }
            _bucketName = bucketName
            s3Util
        }

        def getBucket:String = {
            if ("" equals _bucketName){
                throw new Exception("Bucket hasn't been set yet ! ")
            }
            _bucketName
        }

        def getArchiveBucket:String = {
            if ("" equals _archiveBucketName )
                s3Client.listBuckets().map(b => b.getName).find(b => b.equals(getBucket + "-archive"))
                    .getOrElse(getBucket)
            else
                _archiveBucketName
        }


        System.setProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY, "true")
        /**
          * @TODO get the client of S3
          */
        val s3Client:AmazonS3 = AmazonS3ClientBuilder.standard
            .withCredentials(InstanceProfileCredentialsProvider.getInstance)
            .withEndpointConfiguration(new EndpointConfiguration("s3.cn-north-1.amazonaws.com.cn", "cn-north-1"))
            .build

        /**
          * @TODO get all objects (with no truncate) under the path
          * @param bucketName
          * @param prefix
          * @return
          */
        def s3ObjectSummariesWithNoTruncate(bucketName:String,prefix:String):List[S3ObjectSummary] = {
            import scala.collection.JavaConversions.iterableAsScalaIterable
            var batch = s3Client.listObjectsV2(bucketName,prefix)
            var objectSummaries:MutableList[S3ObjectSummary] = MutableList[S3ObjectSummary]()
            objectSummaries ++= batch.getObjectSummaries.toList

            while(batch.isTruncated){
                batch = s3Client.listObjectsV2(new ListObjectsV2Request()
                    .withBucketName(bucketName)
                    .withPrefix(prefix)
                    .withContinuationToken(batch.getNextContinuationToken)
                )
                objectSummaries ++= batch.getObjectSummaries.toList
            }
            objectSummaries.toList
        }

        /**
          * @TODO validate whether the object on S3 exists already . Waiting for 3 seconds totally at most .
          * @param bucketName
          * @param objectName
          * @return
          */
        def validateObjectExistX3(bucketName: String, objectName: String):Boolean = {
            if (s3Client.doesObjectExist(bucketName,objectName))
                return true
            else
                Thread.sleep(1000)

            if (s3Client.doesObjectExist(bucketName,objectName))
                return true
            else
                Thread.sleep(2000)

            if (s3Client.doesObjectExist(bucketName,objectName))
                return true
            else
                throw new AWSOperationException(s"${bucketName}/${objectName} does not exist yet !")
        }


    }
}