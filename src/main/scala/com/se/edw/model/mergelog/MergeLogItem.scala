package com.se.edw.model.mergelog

import com.se.edw.env._

import scala.collection.mutable

object MergeLogItem extends EnvBasic{

    def apply(commitInfo: CommitInfo, handdleInfos: List[HandleInfo]): MergeLogItem
        = new MergeLogItem(commitInfo, handdleInfos)

    def apply(content:String): MergeLogItem = {
        var c:CommitInfo = null
        val a = mutable.MutableList[HandleInfo]()
        content.split("\\n").foreach(line => jsonMapper.readTree(line) match {
            case x if x.has("commitInfo") => c = new CommitInfo(x)
            case y if y.has("add") => a += new HandleInfo(y)
            case _ =>
        })
        new MergeLogItem(c,a.toList)
    }
}

class MergeLogItem(val commitInfo:CommitInfo, val handdleInfos:List[HandleInfo]){

    private val commitNode = commitInfo.toObjectNode
    private val handleNodes = handdleInfos.map(_.toObjectNode)

    def getContent = {
        (
            List(commitNode.toString) ++
                handleNodes.map(a => "\n" + a.toString)
            ).mkString("")
    }

}
