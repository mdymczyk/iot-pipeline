name := "MyProject"
version := "1.0"
scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
