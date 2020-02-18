package com.se.edw.model.taglog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._

case class RemoveInfo(path:String) {
    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("remove").get("path").asText()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("remove"
            ,jsonMapper.createObjectNode()
                .put("path",path)
        ).asInstanceOf[ObjectNode]
    }
}
