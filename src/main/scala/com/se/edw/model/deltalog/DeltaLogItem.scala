package com.se.edw.model.deltalog

import com.se.edw.env._

object DeltaLogItem extends EnvBasic {

    def apply(commitInfo: CommitInfo,protocalInfo: Option[ProtocalInfo],metaDataInfo: Option[MetaDataInfo] ,
        addInfos: Option[List[AddInfo]],removeInfos: Option[List[RemoveInfo]]):DeltaLogItem
    = new DeltaLogItem(commitInfo,protocalInfo,metaDataInfo,addInfos,removeInfos)

    def apply(content:String): DeltaLogItem = {
        var c:CommitInfo = null
        var p:Option[ProtocalInfo] = None
        var m:Option[MetaDataInfo] = None
        var a:Option[List[AddInfo]] = None
        var r:Option[List[RemoveInfo]] = None
        content.split("\\n").foreach(line => jsonMapper.readTree(line) match {
            case x1 if x1.has("commitInfo") => c = new CommitInfo(x1)
            case x2 if x2.has("protocol") => p = Some(new ProtocalInfo(x2))
            case x3 if x3.has("metaData") => m = Some(new MetaDataInfo(x3))
            case x4 if x4.has("add") => a = Some(a.getOrElse(List()) ++ List(new AddInfo(x4)))
            case x5 if x5.has("remove") => r = Some(r.getOrElse(List()) ++ List(new RemoveInfo(x5)))
            case _ =>
        })
        new DeltaLogItem(c , p , m , a , r)
    }
}

class DeltaLogItem(val commitInfo:CommitInfo, val protocalInfo: Option[ProtocalInfo], val metaDataInfo: Option[MetaDataInfo],
                   val addInfos:Option[List[AddInfo]], val removeInfos:Option[List[RemoveInfo]]){

    private val commitNode = commitInfo.toObjectNode
    private val protocalNode = protocalInfo.map( x => x.toObjectNode ).toList
    private val metaDataNode = metaDataInfo.map( x => x.toObjectNode ).toList
    private val addNodes = addInfos.getOrElse(List[AddInfo]()).map(_.toObjectNode)
    private val removeNodes = removeInfos.getOrElse(List[RemoveInfo]()).map(_.toObjectNode)

    def getContent = {
        (
            List(commitNode.toString) ++
                protocalNode.map(p => "\n" + p.toString) ++
                metaDataNode.map(m => "\n" + m.toString) ++
                addNodes.map(a => "\n" + a.toString) ++
                removeNodes.map(r => "\n" + r.toString)
            ).mkString("")
    }



}
