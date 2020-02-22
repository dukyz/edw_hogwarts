package com.se.edw.env

import com.amazonaws.services.s3.AmazonS3
import org.mongodb.scala.MongoClient


trait EnvMongo extends EnvBasic {

    val mongoUtil = EnvMongo.mongoUtil
    val mongo = mongoUtil.getMongo

}

object EnvMongo {
    object mongoUtil {

        private var _mongoClient: MongoClient = null

        def setMongo(host:String,port:String) :mongoUtil.type = {
            if (_mongoClient != null) {
                throw new Exception("MongoClient has been set already ! ")
            }
            _mongoClient = MongoClient(s"mongodb://${host}:${port}")
            mongoUtil
        }

        def getMongo:MongoClient = {
            if (_mongoClient == null){
                throw new Exception("MongoClient hasn't been set yet ! ")
            }
            _mongoClient
        }






    }
}

