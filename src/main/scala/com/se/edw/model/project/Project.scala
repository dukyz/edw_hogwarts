package com.se.edw.model.project

import org.mongodb.scala.bson.ObjectId


case class Project(var _id: Option[ObjectId], name: String, pm: String, sa: String, vendor: Option[String]) {

}