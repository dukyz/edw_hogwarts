package com.se.edw.model.taglog

import com.se.edw.env._

object TagLogItem extends EnvAWS{

    def apply(commitInfo:CommitInfo , addInfos: List[AddInfo],removeInfos: Option[List[RemoveInfo]]=None): TagLogItem
        = new TagLogItem(commitInfo,addInfos,removeInfos)

    def apply(content:String): TagLogItem = {
        var c:CommitInfo = null
        var a = List[AddInfo]()
        var r = None
        content.split("\\n").foreach(line => jsonMapper.readTree(line) match {
            case z if z.has("commitInfo") => c = new CommitInfo(z)
            case x if x.has("add") => a = a :+ new AddInfo(x)
            case y if y.has("remove") =>  Some(r.getOrElse(List()) :+ new RemoveInfo(y))
            case _ =>
        })
        new TagLogItem(c,a,r)
    }
}

class TagLogItem (val commitInfo:CommitInfo ,val addInfos:List[AddInfo],val removeInfos: Option[List[RemoveInfo]]=None){

    private val commitNode = commitInfo.toObjectNode
    private val addNodes = addInfos.map(_.toObjectNode)
    private val removeNodes = removeInfos.getOrElse(List[RemoveInfo]()).map(_.toObjectNode)

    def getContent = {
        (
            List(commitNode.toString) ++
                addNodes.map(a => "\n" + a.toString) ++
                removeNodes.map(r => "\n" + r.toString)
            ).mkString("")
    }

}
