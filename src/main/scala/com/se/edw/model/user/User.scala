package com.se.edw.model.user

case class User(username: String, password: String = "", token: Option[String] = None) {

}
