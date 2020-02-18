package com.se.edw
import doobie.implicits._
import org.codehaus.jackson.node.{NullNode, ObjectNode}
import scala.collection.JavaConversions.asJavaCollection

object MysqlHandler {
    def apply(): MysqlHandler = new MysqlHandler()
}

class MysqlHandler extends MetadataHandler {

    override def extractMetadata(schemaName:String,originModelName:String): List[ModelField] = {

        val ssql = sql"""select c.COLUMN_NAME as name,
                case c.column_key when 'PRI' then 1 else 0 end as keytype ,
                case c.is_nullable when 'YES' then 1 else 0 end as nulltype ,
                c.data_type as datatype,
                ifnull(c.CHARACTER_MAXIMUM_LENGTH,c.NUMERIC_PRECISION) as prec,
                c.NUMERIC_SCALE as scale,
                c.COLUMN_COMMENT as description
            from information_schema.tables t
            inner join information_schema.COLUMNS c on t.TABLE_NAME = c.TABLE_NAME
                and t.TABLE_SCHEMA = c.TABLE_SCHEMA
                and t.table_schema = ${schemaName}
                and t.table_name = ${originModelName}
            order by c.ordinal_position"""

        ssql.query[ModelField].nel.transact(XA).unsafeRunSync().toList

    }

}
