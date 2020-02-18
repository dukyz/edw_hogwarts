name := "schema-file-generator"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions += "-Ypartial-unification"
resolvers += "HandChina RDC" at "http://nexus.saas.hand-china.com/content/repositories/rdc/"


libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "org.tpolecat" %% "doobie-core" % "0.7.0"
libraryDependencies += "com.oracle" % "ojdbc7" % "12.1.0.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47"
libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13"


