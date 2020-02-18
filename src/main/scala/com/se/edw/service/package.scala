package com.se.edw
import com.se.edw.env._
import com.se.edw.model.domain.DomainObjectSchema
import org.apache.spark.sql.SparkSession

package object service {

    case class ServiceInvoke(
        service:String = "",
        domainNames:Seq[String] = Seq[String](),
        bucket:String = "",
        options:Map[String, String] = Map[String, String]()
    )

    trait EDWService extends EnvEDW{

        /**
          * @TODO What do you do when you wave the magic wand to do something ? Say "Biu ! Biu! Biu!"
          *      This is just the invocation of the EDW Service .
          * @param serviceInvoke
          */
        def biu(serviceInvoke: ServiceInvoke):Unit

        /**
          * @TODO Check the validation of input params of serviceInvoke .
          * @param serviceInvoke
          * @return Messages of invalidation
          */
        def preCheck(serviceInvoke: ServiceInvoke):Option[String] = {
            None
        }


        /**
          * @TODO Initialize resource using in main function . Such as adding configs of spark object .
          * @param serviceInvoke
          * @param spark
          */
        def init(serviceInvoke:ServiceInvoke)(implicit spark:SparkSession = null):Unit = { }


        /**
          * @TODO  Release resources using in main function . such as sparkSession and so on ...
          * @param serviceInvoke
          * @param spark
          */
        def destroy(serviceInvoke:ServiceInvoke)(implicit spark:SparkSession = null):Unit = {
            if (spark != null)
                spark.stop()
        }

        /**
          * @TODO Main function when handling domain object in this service invocation .
          * @param serviceInvoke
          * @param schemaObject
          * @param spark
          */
        def handle(serviceInvoke: ServiceInvoke)
                  (implicit schemaObject : DomainObjectSchema = null, spark : SparkSession = null):Unit

    }

    /**
      * @TODO Simple App trait . Used for common jar invoked by java cmd in System Level
      */
    trait SimpleApp extends EDWService with EnvAWS {

        final override def biu(serviceInvoke: ServiceInvoke): Unit = {
            init(serviceInvoke)
            serviceInvoke.domainNames.map(dm => s"${Domain_Area.Meta_area}/${dm}/_.schema")
                .foreach( schema => {
                    implicit val schemaObject = DomainObjectSchema(s3Util.getBucket,schema)
                    handle(serviceInvoke)
                })
            destroy(serviceInvoke)
        }

    }


    /**
      * @TODO Spark App trait . Used for spark jar invoked by spark-submit cmd .
      */
    trait SparkApp extends EDWService with EnvAWS {

        final override def biu(serviceInvoke: ServiceInvoke): Unit = {
            implicit val spark = SparkSession.builder
                .appName(s"${this.getClass}/${serviceInvoke.bucket}/${serviceInvoke.domainNames}/${serviceInvoke.options}")
                .getOrCreate
            init(serviceInvoke)
            serviceInvoke.domainNames.map(dm => s"${Domain_Area.Meta_area}/${dm}/_.schema")
                .foreach( schema => {
                    implicit val schemaObject = DomainObjectSchema(s3Util.getBucket,schema)
                    handle(serviceInvoke)
                })
            destroy(serviceInvoke)
        }
    }

}