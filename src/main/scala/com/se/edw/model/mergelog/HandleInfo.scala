package com.se.edw.model.mergelog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._

case class HandleInfo(deltaVersion:Long,
                      path:String,
                      size:Long,
                      handleTime:Long=calendar.getTimeInMillis){

    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("add").get("deltaVersion").asLong(),
            jsonNode.get("add").get("path").asText(),
            jsonNode.get("add").get("size").asLong(),
            jsonNode.get("add").get("handleTime").asLong()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("add"
            ,jsonMapper.createObjectNode()
                .put("deltaVersion",deltaVersion)
                .put("path",path)
                .put("size",size)
                .put("handleTime",handleTime)
        ).asInstanceOf[ObjectNode]
    }
}
