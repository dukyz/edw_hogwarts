package com.se.edw.model.deltalog

import java.util.UUID

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.se.edw.env.EnvBasic._

import scala.collection.JavaConversions.asScalaIterator

case class MetaDataInfo(schemaString:String,
                        id:UUID=UUID.randomUUID(),
                        format_provider:String="parquet",
                        format_options:ObjectNode=jsonMapper.createObjectNode(),
                        partitionColumns:ArrayNode=jsonMapper.createArrayNode(),
                        configuration:ObjectNode=jsonMapper.createObjectNode(),
                        createdTime:Long=calendar.getTimeInMillis) {

    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("metaData").get("schemaString").asText(),
            UUID.fromString(jsonNode.get("metaData").get("id").asText()),
            jsonNode.get("metaData").get("format").get("provider").asText(),
            jsonNode.get("metaData").get("format").get("options").asInstanceOf[ObjectNode],
            jsonMapper.createArrayNode().addAll(
                collection.JavaConversions.asJavaCollection(
                    jsonNode.get("metaData").get("partitionColumns").iterator().toList
                )
            ),
            jsonNode.get("metaData").get("configuration").asInstanceOf[ObjectNode],
            jsonNode.get("metaData").get("createdTime").asLong()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("metaData"
            ,jsonMapper.createObjectNode()
                .put("id",id.toString)
                .set("format",jsonMapper.createObjectNode()
                    .put("provider",format_provider)
                    .set("options",format_options)
                ).asInstanceOf[ObjectNode]
                .put("schemaString",schemaString)
                .set("partitionColumns",partitionColumns).asInstanceOf[ObjectNode]
                .set("configuration",configuration).asInstanceOf[ObjectNode]
                .put("createdTime",createdTime)
        ).asInstanceOf[ObjectNode]
    }
}
