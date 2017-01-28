name := "MyProject"
version := "1.0"
scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0",
  "ai.h2o" % "h2o-genmodel" % "3.10.2.2",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
)
