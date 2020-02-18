package com.se.edw.env

import java.util.Calendar

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper

trait EnvBasic {

    /**
      * Common Variables
      */
    val calendar = EnvBasic.calendar
    val jsonMapper = EnvBasic.jsonMapper
}


object EnvBasic {

    /**
      * Common Variables
      */
    val calendar = Calendar.getInstance()
    val jsonMapper = new ObjectMapper()
    jsonMapper.enable(JsonParser.Feature.ALLOW_COMMENTS)

}
