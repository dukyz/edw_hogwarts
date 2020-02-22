package com.se.edw.model.process

import com.fasterxml.jackson.databind.JsonNode
import com.se.edw.env._

import scala.collection.JavaConversions.asScalaIterator

object ProcessObject {
    def apply(srcBucket: String, srcKey: String): ProcessObject
    = new ProcessObject(srcBucket, srcKey)
}

class ProcessObject(srcBucket : String, srcKey:String) extends EnvBasic with EnvAWS {

    private val processString = s3Client.getObjectAsString(srcBucket ,srcKey)

    val process:JsonNode = jsonMapper.readTree(
        processString
            .replaceAll("\n","")
            .replaceAll("\r","")
            .replaceAll("\t"," ")
    )

    val imports:List[P_Import] = process.get("imports").iterator().toList.map(j => {
        P_Import(
            j.get("domain_name").asText,
            j.get("area").asText,
            j.get("model").asText,
            j.get("view").asText,
            if (j.has("format")) j.get("format").asText("delta") else "delta"
        )
    })

    val steps:List[P_Step] = process.get("steps").iterator().toList.map(j => {
        P_Step(
            j.get("sql").asText(""),
            j.get("view").asText
        )
    })

    val exports:List[P_Export] = process.get("exports").iterator().toList.map(j => {
        P_Export(
            j.get("domain_name").asText,
            j.get("area").asText,
            j.get("model").asText,
            j.get("view").asText,
            if (j.has("format")) j.get("format").asText("csv") else "csv",
            if (j.has("compression")) j.get("compression").asText("none") else "none"
        )
    })

    /**
      * @TODO : Get the path of the model in dest functional area of another domain_name
      * @param domainName
      * @param destArea
      * @param subHierarchy
      * @return
      */
    def shiftToAnotherArea(domainName:String,destArea:String,subHierarchy:String*):String = {
        domainName.split("/").+:(destArea).++(subHierarchy).mkString("/")
    }

    /**
      * @TODO : Get the full path of the model in dest functional area of another domain_name
      * @param domainName
      * @param destArea
      * @param subHierarchy
      * @return
      */
    def shiftToAnotherAreaWithFullPath(domainName:String,destArea:String,subHierarchy:String*):String = {
        "s3://" + s3Util.getBucket + "/" + shiftToAnotherArea(domainName,destArea,subHierarchy:_*)
    }

}
