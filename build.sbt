name := "CQRS"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies += "io.projectreactor.kafka" % "reactor-kafka" % "1.3.0"
libraryDependencies += "io.projectreactor" % "reactor-core" % "3.4.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.6.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.6.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3" % Test


