package com.se.edw.service

import com.se.edw.model.domain.DomainObjectSchema
import com.se.edw.service.modelTransformer._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

object ModelTransformer extends SparkApp{

    override def preCheck(serviceInvoke: ServiceInvoke): Option[String] = {
        val options = serviceInvoke.options
        List(
            options.get("snapDate").foldLeft(Some("--option : snapDate is required ! "):Option[String]){ ( _ , value) =>
                None
            },
            options.get("batch").foldLeft(None:Option[String]){ ( _ , value) =>
                if ( StringUtils.isNumeric(value) && Integer.valueOf(value) > 0 )
                    None
                else
                    Some("--option : batch should be an integer greater than zero !")
            },
            super.preCheck(serviceInvoke)
        ).filter( _.isDefined).:+(None).head
    }

    override def init(serviceInvoke: ServiceInvoke)(implicit spark: SparkSession): Unit = {
        spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
    }


    override def handle(serviceInvoke: ServiceInvoke)(implicit schemaObject: DomainObjectSchema, spark: SparkSession) = {

        schemaObject.etlMode.toLowerCase match {
            case "append" => SnapshotMakerByAppend.make(schemaObject,serviceInvoke.options)
            case "merge" => SnapshotMakerByMerge.make(schemaObject,serviceInvoke.options)
            case _ =>
        }
    }
}