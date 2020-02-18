package com.se.edw.model.domain

import com.fasterxml.jackson.databind.JsonNode
import com.se.edw.env._

import scala.collection.JavaConversions.asScalaIterator

object DomainObjectSchema {
    def apply(srcBucket: String, srcKey: String): DomainObjectSchema
        = new DomainObjectSchema(srcBucket, srcKey)
}

class DomainObjectSchema(srcBucket : String, srcKey:String) extends EnvEDW with EnvAWS{

    private val _schemaString = s3Client.getObjectAsString(srcBucket ,srcKey)

    private var _domainObject:DomainObject = null

    private var _archiveObject:ArchiveObject = null

    val bucketLocation = s3Util.getBucket

    val archiveBucketLocation = s3Util.getArchiveBucket

    val schema:JsonNode = jsonMapper.readTree(_schemaString)

    val etlMode:String = schema.get("etlmode").asText()

    val domainName:String  = schema.get("domain_name").asText()

    val mainModelType = etlMode.toLowerCase match {
        case "overwrite" => Domain_Model.Snapshot
        case _ => Domain_Model.Delta
    }

    lazy val pks:List[String] = fields.filter(f => f.pk).map(_.fieldName)

    lazy val fields:List[S_Field]= schema.get("fields").iterator().toList.map(j => {
        val vtype = j.get("type").iterator().toList
        val nullable = if("null".equals(vtype.head.asText)) true else false
        val fieldType = vtype.last.asText
        S_Field(
            j.get("name").asText,
            fieldType,
            nullable,
            j.get("doc").asText,
            null,
            if(j.has("pk")) j.get("pk").asBoolean else false
        )
    })

    lazy val outputs:List[S_Output] = schema.get("_output").iterator().toList.map(j => {
        S_Output(
            j.get("to_service").asText(),
            j.get("from_model").asText(),
            j.get("filter").asText(),
            j.get("shrink").iterator().toList.map(_.asText()),
            if(j.has("format")) j.get("format").asText() else "csv",
            if(j.has("compression")) j.get("compression").asText() else "none"
        )
    })

    lazy val model:S_Model = if (! schema.has("_model")) S_Model().setEmpty() else {
        S_Model(
            {
                val ss = schema.get("_model").get("snapshot")
                Map[String,Any](
                    "partitionBy" -> {
                        if (ss.has("partitionBy"))
                              ss.get("partitionBy").iterator().toList.map(_.asText())
                        else
                              List[String]()
                    } , "replaceBy" -> {
                        if (ss.has("replaceBy"))
                            ss.get("replaceBy").iterator().toList.map(_.asText())
                        else
                            List[String]()
                    }
                )
            },{
                Map[String,Any]()
            }
        )
    }


    /**
      * @TODO Get the corresponding domain object
      * @return
      */
    def getDomainObjectInstance():DomainObject = {
        if (this._domainObject != null)
            this._domainObject
        else{
            DomainObject(this)
        }
    }

    /**
      * @TODO Get the corresponding archive object
      * @return
      */
    def getArchiveObjectInstance():ArchiveObject = {
        if (this._archiveObject != null)
            this._archiveObject
        else {
            ArchiveObject(this)
        }
    }

    /**
      * @TODO : Get the path of the model in dest functional area
      * @param destArea
      * @param subHierarchy
      * @return
      */
    def shiftToArea(destArea:String,subHierarchy:String*):String = {
        domainName.split("/").+:(destArea).++(subHierarchy.filter(s => !s.equals("") && s != null)).mkString("/")
    }

    /**
      * @TODO : Get the full path of the model in dest functional area
      * @param destArea
      * @param subHierarchy
      * @return
      */
    def shiftToAreaWithFullPath(destArea:String,subHierarchy:String*):String = {
        "s3://" + s3Util.getBucket + "/" + shiftToArea(destArea,subHierarchy:_*)
    }

}
