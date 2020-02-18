name := "edw_hogwarts"

version := "0.1.0"

scalaVersion := "2.11.12"

lazy val env = (project in file("."))
//lazy val parquetFileGenerator = (project in file("parquet-file-generator"))
lazy val modelInitializer= (project in file("model-initializer")).dependsOn(env)

assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.last
    //    case PathList("org", "commons", xs @ _*)         => MergeStrategy.last
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "mime.types" => MergeStrategy.last
    case x => (assemblyMergeStrategy in assembly).value(x)
}


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"  % "provided"
libraryDependencies += "io.delta" %% "delta-core" % "0.4.0" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.595"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"



scalacOptions += "-feature"
