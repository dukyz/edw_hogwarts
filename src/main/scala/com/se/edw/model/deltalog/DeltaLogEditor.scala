package com.se.edw.model.deltalog

import com.se.edw.env._
import scala.collection.JavaConversions.iterableAsScalaIterable

object DeltaLogEditor {

    def apply(objectPath: String): DeltaLogEditor = new DeltaLogEditor(objectPath)

    case object operationType {
        val WRITE = "WRITE"
    }

    case object operationMode {
        val Overwrite = "Overwrite"
        val Append = "Append"
        val ErrorIfExists = "ErrorIfExists"
    }

}

class DeltaLogEditor(objectPath:String ) extends EnvAWS {
    lazy val logList = s3Util.s3ObjectSummariesWithNoTruncate(s3Util.getBucket,objectPath + "/" + "_delta_log")
        .filter(s => s.getKey.endsWith("json"))
        .map(s => s.getKey.split("/").last).toList

    lazy val lastVersion = logList.sortBy(x => x).last.split("\\.").head.toInt

    lazy val lastLog:DeltaLogItem = getLogByVersion(lastVersion)

    def putLog(version:Long ,log:DeltaLogItem) = {
        s3Client.putObject(s3Util.getBucket,
            objectPath + "/_delta_log/" + getLogName(version),
            log.getContent)
    }

    def getLogByVersion(version : Long):DeltaLogItem = {
        DeltaLogItem(
            s3Client.getObjectAsString(s3Util.getBucket,objectPath + "/_delta_log/" + getLogName(version))
        )
    }

    def getLogName(version:Long):String = {
        (version.toString.length+1 to 20).map(_ => "0").mkString + version + ".json"
    }


}
