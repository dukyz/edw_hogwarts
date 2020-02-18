package com.se.edw.model.taglog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._

case class CommitInfo(timestamp:Long=calendar.getTimeInMillis){
    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("commitInfo").get("timestamp").asLong()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("commitInfo"
            ,jsonMapper.createObjectNode
                .put("timestamp",timestamp )
        ).asInstanceOf[ObjectNode]
    }

}
