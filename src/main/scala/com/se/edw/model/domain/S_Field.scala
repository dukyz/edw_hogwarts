package com.se.edw.model.domain

case class S_Field(fieldName:String, filedType:String="string"
                   , nullable:Boolean=true, doc:String="", default:Any=null, pk:Boolean=false){

}
