package com.se.edw.model.deltalog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._

case class AddInfo(path:String,
              size:Long,
              modificationTime:Long=calendar.getTimeInMillis,
              dataChange:Boolean=true,
              partitionValues:ObjectNode=jsonMapper.createObjectNode()) {

    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("add").get("path").asText(),
            jsonNode.get("add").get("size").asLong(),
            jsonNode.get("add").get("modificationTime").asLong(),
            jsonNode.get("add").get("dataChange").asBoolean(),
            jsonNode.get("add").get("partitionValues").asInstanceOf[ObjectNode]
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("add"
            ,jsonMapper.createObjectNode()
                .put("path",path)
                //remember to fix it !!
                .set("partitionValues",partitionValues)
                .asInstanceOf[ObjectNode]
                .put("size",size)
                .put("modificationTime",modificationTime)
                .put("dataChange",dataChange)
        ).asInstanceOf[ObjectNode]
    }
}
