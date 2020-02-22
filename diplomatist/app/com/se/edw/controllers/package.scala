package com.se.edw

import java.math.BigInteger
import java.security.MessageDigest

import com.se.edw.env.EnvMongo
import com.se.edw.model.project.Project
import com.se.edw.model.user.User
import com.se.edw.model.web.Response
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import play.api.libs.json.Reads._
import play.api.libs.json._

package object controllers extends EnvMongo {

    implicit val objectIdWrites: Writes[ObjectId] = new Writes[ObjectId] {
        def writes(objectId: ObjectId) = Json.toJson(objectId.toString)
    }
    implicit val objectIdReads: Reads[ObjectId] = {
        (JsPath.read[String]).map(x => new ObjectId(x))
    }
    implicit val objectIdFormat: Format[ObjectId] = Format(objectIdReads, objectIdWrites)

    def json[T](o: T)(implicit tjs: Writes[T]): JsValue = Json.toJson(o)

    def userTokenGenerate(user: User): String = {
        new BigInteger(1,
            MessageDigest.getInstance("MD5").digest(user.username.getBytes)
        ).toString(16)
    }

    def userTokenCheck(user: User): Boolean = {
        userTokenGenerate(user).equals(user.token)
    }

    implicit val responseFormat = Json.format[Response]
    implicit val projectFormat = Json.format[Project]
    implicit val userFormat = Json.format[User]


    val mongoCodecRegistry = fromRegistries(
        fromProviders(classOf[Project]),
        fromProviders(classOf[User]),
        DEFAULT_CODEC_REGISTRY
    )
    val edw = mongo.getDatabase("edw").withCodecRegistry(mongoCodecRegistry)

}
