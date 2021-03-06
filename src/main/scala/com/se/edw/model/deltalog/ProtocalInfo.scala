package com.se.edw.model.deltalog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._

case class ProtocalInfo(minReaderVersion:Int=1,minWriterVersion:Int=2) {
    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("protocol").get("minReaderVersion").asInt(),
            jsonNode.get("protocol").get("minWriterVersion").asInt()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("protocol"
            ,jsonMapper.createObjectNode()
                .put("minReaderVersion",minReaderVersion)
                .put("minWriterVersion",minWriterVersion)
        ).asInstanceOf[ObjectNode]
    }

}
