package com.se.edw.model.mergelog

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.se.edw.env.EnvBasic._
import com.se.edw.model.mergelog.MergeLogEditor._

case class CommitInfo(timestamp:Long=calendar.getTimeInMillis,
                      operation:String=operationType.WRITE,
                      mode:String=operationMode.Overwrite ,
                      snapshotDate:String=new SimpleDateFormat("yyyy-MM-dd")
                          .format(new Date(calendar.getTimeInMillis))
                ){
    def this(jsonNode:JsonNode) = {
        this(
            jsonNode.get("commitInfo").get("timestamp").asLong(),
            jsonNode.get("commitInfo").get("operation").asText(),
            jsonNode.get("commitInfo").get("mode").asText(),
            jsonNode.get("commitInfo").get("snapshotDate").asText()
        )
    }

    def toObjectNode = {
        jsonMapper.createObjectNode().set("commitInfo"
            ,jsonMapper.createObjectNode
                .put("timestamp",timestamp )
                .put("operation",operation )
                .put("mode",mode )
                .put("snapshotDate",snapshotDate)
        ).asInstanceOf[ObjectNode]
    }

}
