package com.se.edw.model.web

import play.api.http.Status
import play.api.libs.json.{JsNull, JsValue}

object ResponseError {
    def apply(status: Int, error: String, data: JsValue = JsNull): Response =
        new Response(status, "", error, data)
}

object ResponseSuccess {
    def apply(success: String, data: JsValue): Response =
        new Response(Status.OK, success, "", data)
}

case class Response(status: Int, success: String = "", error: String = "", data: JsValue = JsNull) {

}
