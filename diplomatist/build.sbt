name := "diplomatist"

version := "0.1.0"

scalaVersion := "2.11.12"

libraryDependencies += guice

assemblyMergeStrategy in assembly := {
    case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.last
    //    case PathList("org", "commons", xs @ _*)         => MergeStrategy.last
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "mime.types" => MergeStrategy.last
//    case PathList(ps @ _*) if ps.last endsWith ".conf" => MergeStrategy.last
    case x => (assemblyMergeStrategy in assembly).value(x)
}

