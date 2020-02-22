package com.se.edw.controllers

import com.se.edw.model.project.Project
import javax.inject._
import org.mongodb.scala._
import org.mongodb.scala.bson.ObjectId
import play.api.libs.json.Json
import play.api.mvc._

import scala.concurrent.ExecutionContext

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class ProjectController @Inject()(cc: ControllerComponents)(implicit ec :ExecutionContext) extends AbstractController(cc) {


    val projects:MongoCollection[Project] = edw.getCollection("project")

    def get = Action.async {

        projects.find().toFuture().map( x => {
            Ok(Json.toJson(x))
        })

    }


    def post = Action.async {request => {


        val reqBody = request.body.asJson.get.as[Project]

        val proj = reqBody._id.foldLeft({
            reqBody.copy( _id = Some(new ObjectId()))
        })((_,_) => {
            reqBody
        })

        projects.insertOne(proj).toFuture().map( _ => Ok(Json.toJson(proj)) )

    }
    }

}
