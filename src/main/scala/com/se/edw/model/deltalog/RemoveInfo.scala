package com.se.edw.model.deltalog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._

case class RemoveInfo(path:String,
                 deletionTimestamp:Long=calendar.getTimeInMillis,
                 dataChange:Boolean=true)  {

    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("remove").get("path").asText(),
            jsonNode.get("remove").get("deletionTimestamp").asLong(),
            jsonNode.get("remove").get("dataChange").asBoolean()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("remove"
            ,jsonMapper.createObjectNode()
                .put("path",path)
                .put("deletionTimestamp",deletionTimestamp)
                .put("dataChange",dataChange)
        ).asInstanceOf[ObjectNode]
    }
}
