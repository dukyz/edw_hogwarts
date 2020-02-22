package com.se.edw.model.mergelog

import com.se.edw.env._
import org.apache.commons.lang3.StringUtils

object MergeLogEditor {

    def apply(objectPath: String): MergeLogEditor = new MergeLogEditor(objectPath)

    case object operationType {
        val WRITE = "WRITE"
    }

    case object operationMode {
        val Overwrite = "Overwrite"
        val Append = "Append"
        val Merge = "Merge"
        val ErrorIfExists = "ErrorIfExists"
    }

}

class MergeLogEditor(objectPath:String ) extends EnvAWS{
    lazy val logList = s3Util.s3ObjectSummariesWithNoTruncate(s3Util.getBucket,objectPath + "/" + "_merge_log")
        .map(s => s.getKey.split("/").last).toList

    lazy val lastVersion = logList.sortBy(x => x).last.split("\\.").head.toInt

    lazy val lastLog:MergeLogItem = getLogByVersion(lastVersion)


    def putLog(version:Long ,log:MergeLogItem) = {
        s3Client.putObject(s3Util.getBucket,
            objectPath + "/_merge_log/" + getLogName(version),
            log.getContent)
    }

    def getLogByVersion(version : Long):MergeLogItem = {
        MergeLogItem(
            s3Client.getObjectAsString(s3Util.getBucket,objectPath + "/_merge_log/" + getLogName(version))
        )
    }

    def getLogName(version:Long):String = {
        StringUtils.leftPad(version.toString,20,"0") + ".json"
    }

}
