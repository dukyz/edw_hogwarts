package com.se.edw.model.taglog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._

case class AddInfo(path:String) {
    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("add").get("path").asText()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("add"
            ,jsonMapper.createObjectNode()
                .put("path",path)
        ).asInstanceOf[ObjectNode]
    }
}
