package com.se.edw

import com.se.edw.env._
import com.se.edw.service.ServiceInvoke
import scopt.OParser

object Wand extends EnvAWS{

    def main(args: Array[String]): Unit = {

        val servicePackage = "com.se.edw.service"
        val builder = OParser.builder[ServiceInvoke]
        import builder._
        val parser = OParser.sequence(
            programName("EDW Service MagicWand"),
            head("EDW Service MagicWand", "0.1"),

            opt[String]('s', "service")
                .required()
                .action((x, c) => c.copy(service = x))
                .text("services to run"),

            opt[Seq[String]]('d',"domainName")
                .required()
                .action((x,c) => c.copy(domainNames = x))
                .text("domainNames which run with services"),

            opt[String]('b',"bucket")
                .required()
                .action((x,c) => c.copy(bucket = x))
                .text("name of S3 bucket"),

            opt[Map[String,String]]('o',"option")
                .action((x,c) => c.copy(options = x))
                .text("configure of services"),

            checkConfig(
                serviceInvoke => {
                    s3Util.setBucket(serviceInvoke.bucket)

                    Class.forName(s"${servicePackage}.${serviceInvoke.service}")
                        .getDeclaredMethod("preCheck",classOf[ServiceInvoke])
                        .invoke(null,serviceInvoke)
                        .asInstanceOf[Option[String]].foldLeft(success){ (_ , message) =>
                        failure(message)
                    }
                }
            )
        )

        OParser.parse(parser, args, ServiceInvoke()) match {
            case Some(serviceInvoke) => {
                Class.forName(s"${servicePackage}.${serviceInvoke.service}")
                    .getDeclaredMethod("biu",classOf[ServiceInvoke])
                    .invoke(null,serviceInvoke)
            }
            case _ =>
        }
    }
}
