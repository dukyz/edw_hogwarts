name := "ollivander"

version := "0.1.0"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"  % "provided"
libraryDependencies += "io.delta" %% "delta-core" % "0.5.0" % "provided"