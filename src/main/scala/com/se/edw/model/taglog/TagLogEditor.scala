package com.se.edw.model.taglog

import com.se.edw.env._

object TagLogEditor {
    def apply(objectPath: String , tagString:String): TagLogEditor = new TagLogEditor(objectPath ,tagString)
}

class TagLogEditor(val objectPath:String , val tagString:String) extends EnvAWS {
    lazy val logList = s3Util.s3ObjectSummariesWithNoTruncate(s3Util.getBucket,objectPath + "/" + tagString)
        .filter(s=> {
            val logs = s.getKey.split("/")
            !logs(logs.size-2).equals("discarded")
        })
        .map(s => s.getKey.split("/").last).toList

    lazy val lastVersion = if (logList.size==0) 0 else logList.sortBy(x => x).last.split("\\.").head.toInt

    lazy val lastLog:TagLogItem = getLogByVersion(lastVersion )

    lazy val previousLogs:List[TagLogItem] = {
        logList
            .filter(x => x.split("\\.").head.toInt != lastVersion)
            .map(x => TagLogItem(
                s3Client.getObjectAsString(s3Util.getBucket,objectPath + "/" + tagString + "/" + x)
        ))
    }

    def putLog(version:Long ,log:TagLogItem) = {
        s3Client.putObject(s3Util.getBucket,
            objectPath + "/"  + tagString + "/" + getLogName(version),
            log.getContent)
    }

    def discardLog(version:Long ) = {
        s3Client.copyObject(
            s3Util.getBucket,
            objectPath + "/"  + tagString + "/" + getLogName(version),
            s3Util.getBucket,
            objectPath + "/"  + tagString + "/discarded/" + getLogName(version)
        )
        s3Client.deleteObject(
            s3Util.getBucket,
            objectPath + "/"  + tagString + "/" + getLogName(version)
        )
    }

    def getLogByVersion(version : Long ):TagLogItem = {
        TagLogItem(
            s3Client.getObjectAsString(s3Util.getBucket,objectPath + "/" + tagString + "/" + getLogName(version))
        )
    }

    def getLogName(version:Long):String = {
        (version.toString.length+1 to 20).map(_ => "0").mkString + version + ".json"
    }

}
