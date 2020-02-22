import com.google.inject.AbstractModule
import com.se.edw.env.EnvMongo
import com.typesafe.config.ConfigFactory

/**
 * This class is a Guice module that tells Guice how to bind several
 * different types. This Guice module is created when the Play
 * application starts.

 * Play will automatically use any class called `Module` that is in
 * the root package. You can create modules in other locations by
 * adding `play.modules.enabled` settings to the `application.conf`
 * configuration file.
 */
class Module extends AbstractModule {

  override def configure() = {

    val config = ConfigFactory.load()
    val host = config.getString("edw.diplomatist.db.host")
    val port = config.getString("edw.diplomatist.db.port")
    EnvMongo.mongoUtil.setMongo(host,port)

//    // Use the system clock as the default implementation of Clock
//    bind(classOf[Clock]).toInstance(Clock.systemDefaultZone)
//    // Ask Guice to create an instance of ApplicationTimer when the
//    // application starts.
//    bind(classOf[ApplicationTimer]).asEagerSingleton()
//    // Set AtomicCounter as the implementation for Counter.
//    bind(classOf[Counter]).to(classOf[AtomicCounter])
  }

}
