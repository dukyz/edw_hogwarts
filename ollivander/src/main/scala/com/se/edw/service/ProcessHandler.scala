package com.se.edw.service

import com.se.edw.env._
import com.se.edw.model.process.ProcessObject
import com.se.edw.model.domain.DomainObjectSchema
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

//spark-submit --class com.se.edw.ProcessHandler --jars s3://edw-core-dev/_resource/_EMR/_JAR/_SPARK/delta-core_2.11-0.3.0.jar --master yarn --deploy-mode client s3://edw-core-dev/_resource/_EMR/_JAR/_SPARK/process-handler-assembly-0.1.jar  edw-core-dev _process/TEST/DEV/Sample/_.process
object ProcessHandler extends EnvEDW with EnvAWS {

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("ProcessHandler:" + args.tail.mkString(" ")).getOrCreate
        spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
//      val bucketName = "edw-core-dev"
//		val srcKeys = List("_meta/TEST/BW/_BIC_AZAPD_D1600/_.schema")
		val bucketName = args(0)
        val srcKeys = args.tail
        s3Util.setBucket(bucketName)
        import spark.sql

        srcKeys.foreach( srcKey => {
            val processObject = ProcessObject(s3Util.getBucket,srcKey)
            processObject.imports.foreach( i => {
                spark.read
                    .format(i.format)
                    .options({
                        val m = mutable.Map[String,String]()
                        if("csv".equals(i.format.toLowerCase)){
                            m.put("header","true")
                        }
                        m
                    })
                    .load(processObject.shiftToAnotherAreaWithFullPath(i.domainName,i.area,i.model))
                    .createOrReplaceTempView(i.view)
            })

            processObject.steps.foreach( s => {
                sql(s.sql).createOrReplaceTempView(s.view)
            })

            processObject.exports.foreach( e => {
                val output = sql("select * from " + e.view)
                    .write
                    .format(e.format)
                    .mode(SaveMode.Overwrite)
                    .options({
                        val m = mutable.Map[String,String]()
                        if("csv".equals(e.format.toLowerCase)){
                            m.put("quoteAll","true")
                            m.put("header","true")
                        }
                        if("csv".equals(e.format.toLowerCase) & "lzo".equals(e.compression.toLowerCase)) {
                            m.put("codec","com.hadoop.compression.lzo.LzopCodec")
                        } else {
                            m.put("compression",e.compression)
                        }
                        m
                    }).save(processObject.shiftToAnotherAreaWithFullPath(e.domainName,e.area,e.model))
            })
        })
        spark.stop()
	}


//    override def handle(serviceInvoke: ServiceInvoke)(implicit schemaObject: SchemaObject, spark: SparkSession) = ???
}








