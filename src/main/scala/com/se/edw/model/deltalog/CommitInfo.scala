package com.se.edw.model.deltalog

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._
import com.se.edw.model.deltalog.DeltaLogEditor._

case class CommitInfo(readVersion:Int = -1,timestamp:Long=calendar.getTimeInMillis,
                      operation:String=operationType.WRITE,
                 operationParameters_mode:String=operationMode.Overwrite,
                 operationParameters_partitionBy:String="[]",
                 isBlindAppend:Boolean=false
                )  {

    def this(jsonNode:JsonNode) = {
        this(
            if (jsonNode.get("commitInfo").has("readVersion"))
                jsonNode.get("commitInfo").get("readVersion").asInt() else -1 ,
            jsonNode.get("commitInfo").get("timestamp").asLong(),
            jsonNode.get("commitInfo").get("operation").asText(),
            jsonNode.get("commitInfo").get("operationParameters").get("mode").asText(),
            jsonNode.get("commitInfo").get("operationParameters").get("partitionBy").asText(),
            jsonNode.get("commitInfo").get("isBlindAppend").asBoolean()
        )
    }

    def toObjectNode = {
        val x = jsonMapper.createObjectNode().set("commitInfo"
            ,jsonMapper.createObjectNode
                .put("timestamp",timestamp)
                .put("operation",operation)
                .set("operationParameters"
                    ,jsonMapper.createObjectNode
                        .put("mode",operationParameters_mode)
                        //remember to fix it !!
                        .put("partitionBy",operationParameters_partitionBy)
                ).asInstanceOf[ObjectNode]
                .put("isBlindAppend",isBlindAppend)
        ).asInstanceOf[ObjectNode]

        if (readVersion != -1)
            x.get("commitInfo").asInstanceOf[ObjectNode].put("readVersion",readVersion )
        x
    }

}
