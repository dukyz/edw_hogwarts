name := "edw_hogwarts"

version := "0.1.0"

scalaVersion := "2.11.12"

lazy val env = (project in file("."))
lazy val diplomatist= (project in file("diplomatist")).dependsOn(env).enablePlugins(PlayScala)
//lazy val ollivander= (project in file("ollivander")).dependsOn(env)


assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.last
    //    case PathList("org", "commons", xs @ _*)         => MergeStrategy.last
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "mime.types" => MergeStrategy.last
    case x => (assemblyMergeStrategy in assembly).value(x)
}

scalacOptions += "-feature"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.595"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.8.0"



