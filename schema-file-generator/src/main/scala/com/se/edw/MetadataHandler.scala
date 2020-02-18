package com.se.edw

import java.io.{File, PrintWriter}

import cats.effect.IO
import doobie.util.transactor.Transactor
import org.codehaus.jackson.node.{NullNode, ObjectNode}
import scala.collection.JavaConversions.asJavaCollection

trait MetadataHandler {

    def extractMetadata(schemaName:String,originModelName:String):List[ModelField]

    def xaInit(location: ModelLocation) = {
        XA = Transactor.fromDriverManager[IO](
            Cons_DBType.get(location.dbtype).get,
            location.jdbc,
            location.username,
            location.password
        )
    }

    def handle(modelLocation:ModelLocation) = {
        xaInit(modelLocation)
        modelLocation.models.foreach( x => {
            val fields = extractMetadata(modelLocation.subjct,x._1)
            val root = processModel(modelLocation,x._1,fields)
            val schemaJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root)
            val writer = new PrintWriter(new File(s"${x._2}.schema"))
            writer.write(schemaJson)
            writer.close()
        })
    }

    def processModel(modelLocation:ModelLocation,originModelName:String,fields:List[ModelField]): ObjectNode = {
        val root = mapper.createObjectNode()
        root.put("type","record")
        root.put("name",modelLocation.models.get(originModelName).get)
        root.put("etlmode",modelLocation.etlmode)
        val fieldsNode = mapper.createArrayNode()
        fieldsNode.addAll(fields.map( f => {
            val n = mapper.createObjectNode()
            n.put("name",f.name)
            val typeArray = mapper.createArrayNode()
            typeArray.add("null")
            typeArray.add(Cons_DBFieldTypeTransfer.get(f.datatype.toLowerCase()).getOrElse("string"))
            n.put("type",typeArray)
            n.put("doc",f.description.getOrElse(""))
            n.put("default",NullNode.getInstance())
            n.put("pk",if( f.keytype == 1 ) true else false)
            n
        }))
        root.put("fields",fieldsNode)
        root
    }


}


