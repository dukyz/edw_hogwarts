package com.se.edw.service.modelTransformer

import com.se.edw.env._
import com.se.edw.model.domain.DomainObjectSchema
import org.apache.spark.sql.SparkSession

trait ModelMaker extends EnvEDW with EnvAWS{

    /**
      * @TODO : make the snapshot by rules in schema
      * @param schemaObject
      */
    def make(schemaObject:DomainObjectSchema, options:Map[String,String]=null)(implicit spark:SparkSession):Unit


}
