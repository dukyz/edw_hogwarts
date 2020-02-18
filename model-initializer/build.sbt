name := "model-initializer"

version := "0.3"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.parquet" % "parquet-cli" % "1.10.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"

assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith "ConvertCSVCommand.class" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.last
//    case PathList("org", "commons", xs @ _*)         => MergeStrategy.last
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "mime.types" => MergeStrategy.last
    case x => (assemblyMergeStrategy in assembly).value(x)
}