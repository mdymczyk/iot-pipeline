name := "MyProject"
version := "1.0"
scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

resolvers += "mapr" at "http://repository.mapr.com/nexus/content/repositories/releases"
resolvers += "confluentio" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.9.0.0-mapr-1602-streams-5.2.0",
  "ai.h2o" % "h2o-genmodel" % "3.10.2.2",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "io.confluent" % "kafka-json-serializer" % "3.1.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}