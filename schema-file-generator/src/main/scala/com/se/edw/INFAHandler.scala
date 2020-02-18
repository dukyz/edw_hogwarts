package com.se.edw
import doobie.implicits._
import org.codehaus.jackson.node.{NullNode, ObjectNode}


object INFAHandler {
    def apply(): INFAHandler = new INFAHandler()
}

class INFAHandler extends MetadataHandler {

    override def extractMetadata(schemaName:String,originModelName:String): List[ModelField] = {

        val ssql = sql"""select tf.target_name as name,
        case when tf.keytype in (1,3) then 1 else 0 end as keytype ,tf.nulltype,
        fdt.dtype_name as datatype,tf.dprec as prec,tf.dscale as scale,tf.target_desc as descption
        from opb_targ t
        inner join opb_subject s on t.subj_id = s.subj_id
        and s.subj_name = ${schemaName}
        and t.target_name = ${originModelName}
        and t.is_visible = 1
        inner join opb_targ_fld tf on t.target_id = tf.target_id and t.version_number = tf.version_number
        inner join rep_fld_datatype fdt on fdt.dtype_num =  tf.ndtype
        order by tf.fldno"""

        ssql.query[ModelField].nel.transact(XA).unsafeRunSync().toList

    }

}
