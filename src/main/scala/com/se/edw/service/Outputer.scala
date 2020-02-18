package com.se.edw.service

import java.text.SimpleDateFormat
import java.util.Date

import com.se.edw.env._
import com.se.edw.model.domain.DomainObjectSchema
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SaveMode, SparkSession}

import scala.collection.mutable

object Outputer extends SparkApp{

    override def preCheck(serviceInvoke: ServiceInvoke): Option[String] = {
        val options = serviceInvoke.options
        List(
            options.get("timescope").foldLeft(None:Option[String]){ ( _ , value) =>
                if (List("true","false").contains(value.toLowerCase)) None else Some("--option : timescope need to be true or false")
            },
            super.preCheck(serviceInvoke)
        ).filter( _.isDefined).:+(None).head
    }


    override def handle(serviceInvoke: ServiceInvoke)(implicit schemaObject: DomainObjectSchema, spark: SparkSession) = {
        val options = serviceInvoke.options
        schemaObject.outputs.foreach(output => {
            var all = spark.read.format("delta")
                .load("s3://" + s3Util.getBucket + "/" +
                    schemaObject.shiftToArea(Domain_Area.Model_area,output.from_model))
            if (output.filter.length != 0)  {
                all = all.filter(output.filter)
            }
            if (output.shrink.size != 0 ){
                all = all.select(output.shrink.map(new Column(_)):_*)
            }
            if (options.contains("timescope") && options.get("timescope").get.toBoolean){
                all = all
                    .withColumn("effective_ts",lit(new SimpleDateFormat("yyyy-MM-dd")
                        .format(new Date(calendar.getTimeInMillis))))
                    .withColumn("expired_ts",lit("9999-12-31"))
            }
            all.write.format(output.format)
                .mode(SaveMode.Overwrite)
                .options({
                    val m = mutable.Map[String,String]()
                    if("csv".equals(output.format.toLowerCase)){
                        m.put("quoteAll","true")
                        m.put("header","true")
                    }
                    if("csv".equals(output.format.toLowerCase) & "lzo".equals(output.compression.toLowerCase)) {
                        m.put("codec","com.hadoop.compression.lzo.LzopCodec")
                    } else {
                        m.put("compression",output.compression)
                    }
                    m
                })
                .save("s3://" + s3Util.getBucket + "/" +
                    schemaObject.shiftToArea(Domain_Area.Output_area,output.to_service))
        })
    }
}