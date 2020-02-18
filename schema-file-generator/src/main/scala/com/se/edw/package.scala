package com.se

import cats.effect.IO
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import org.codehaus.jackson.map.ObjectMapper


package object edw {
    implicit val cs = IO.contextShift(ExecutionContexts.synchronous)
    var XA:Transactor[IO] = null

    val mapper = new ObjectMapper()

    val Cons_Location:Seq[String]  = Seq("INFA","DB")
    val Cons_EtlMode:Seq[String]  = Seq("APPEND","OVERWRITE","MERGE")
    val Cons_DBType:Map[String,String] = Map(
        "ORACLE" -> "oracle.jdbc.driver.OracleDriver",
        "MYSQL" -> "com.mysql.jdbc.Driver"
    )
    val Cons_DBFieldTypeTransfer:Map[String,String] = Map(
        "string" -> "string",
        "varchar" -> "string",
        "varchar2" -> "string",
        "nvarchar" -> "string",
        "nvarchar2" -> "string",
        "nstring" -> "string",
        "char" -> "string",
        "nchar" -> "string",
        "wchar" -> "string",
        "timestamp" -> "string",
        "date" -> "string",
        "time" -> "string",
        "datetime" -> "string",
        "text" -> "string",
        "ntext" -> "string",
        "money" -> "string",

        "int" -> "int",
        "smallint" -> "int",
        "bit" -> "int",
        "integer" -> "int",
        "tinyint" -> "int",
        "short" -> "int",

        "bigint" -> "long",

        "float" -> "float",

        "double" -> "double",
        "decimal" -> "double",
        "numeric" -> "double",
        "number" -> "double",

        "boolean" -> "boolean",
        "real" -> "boolean",

        "longblob" -> "bytes",
        "byte" -> "bytes",
        "varbinary" -> "bytes",
        "binary" -> "bytes"

    )

    case class ModelLocation (location: String = "INFA",etlmode: String = "OVERWRITE",
                              dbtype: String ="", jdbc:String ="",
                              username:String = "",password:String = "",
                              subjct:String = "" , models: Map[String,String] = Map()
                             )

    case class ModelField(name:String,keytype:Int,nulltype:Int,
                     datatype:String,prec:Option[Long],scale:Option[Int],description:Option[String])
}
