name := "lsr-netty-protocols-base"

organization := "ch.epfl.lsr"

version := "0.1"

scalaVersion := "2.9.2"

scalacOptions += "-deprecation"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Typesafe Snapshots Repository" at "http://repo.typesafe.com/typesafe/snapshots/"

libraryDependencies += "io.netty" % "netty" % "3.5.2.Final"

libraryDependencies += "ch.epfl.lsr" %% "netty-codec-kryo" % "0.1"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0"

libraryDependencies += "com.typesafe" % "config" % "0.5.0"

libraryDependencies += "com.romix.akka" % "akka-kryo-serialization" % "0.1-SNAPSHOT"

fork := true
