name := "lsr-netty-protocols-base"

organization := "ch.epfl.lsr"

version := "0.1"

scalaVersion := "2.10.1"

scalacOptions ++= Seq( "-deprecation", "-feature" )

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Typesafe Snapshots Repository" at "http://repo.typesafe.com/typesafe/snapshots/"

libraryDependencies += "io.netty" % "netty" % "3.7.0.Final"

libraryDependencies += "com.typesafe" % "config" % "0.5.0"

libraryDependencies += "com.esotericsoftware.kryo" % "kryo" % "2.19"

fork := true
