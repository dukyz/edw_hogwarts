package com.se.edw.model.domain

case class S_Output(to_service: String, from_model: String = "snapshot",
                    filter: String = "", shrink: List[String] = List(),
                    format: String = "csv", compression: String = "none") {

}
