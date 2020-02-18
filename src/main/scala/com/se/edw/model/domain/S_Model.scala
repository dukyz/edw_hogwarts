package com.se.edw.model.domain

case class S_Model(snapshot:Map[String,Any] = Map[String,Any](),
                   delta:Map[String,Any] = Map[String,Any]()) {

    private var empty:Boolean = false

    def setEmpty() = {
        this.empty = true
        this
    }

    def isEmpty():Boolean = empty

    def hasPartitionedSnapshot():Boolean = snapshot.contains("partitionBy")

}
