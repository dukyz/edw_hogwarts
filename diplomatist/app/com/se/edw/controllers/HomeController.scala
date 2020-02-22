package com.se.edw.controllers

import com.se.edw.model.user.User
import com.se.edw.model.web.{ResponseError, ResponseSuccess}
import javax.inject._
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters._
import play.api.http
import play.api.mvc.{AbstractController, ControllerComponents}

import scala.concurrent.ExecutionContext

@Singleton
class HomeController @Inject()(cc: ControllerComponents)(implicit ec: ExecutionContext) extends AbstractController(cc) {

    val users: MongoCollection[User] = edw.getCollection("user")

    def login = Action.async { request =>

        val loginUser = request.body.asJson.get.as[User]

        users.find(
            and(
                equal("username", loginUser.username),
                equal("password", loginUser.password)
            )
        ).headOption().map(optionUser => {
            optionUser.foldLeft(
                Ok(json(ResponseError(http.Status.NOT_FOUND, "Authentication Failed!")))
            )(
                (_, user) =>
                    Ok(json(ResponseSuccess(s"Welcome ${user.username}!",
                        json(user.copy(password = "", token = Some(userTokenGenerate(user))))))
                    )
            )
        })
    }



}
