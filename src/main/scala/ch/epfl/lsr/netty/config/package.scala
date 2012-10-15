package ch.epfl.lsr.netty

import com.typesafe.config.{ Config, ConfigFactory }

package object config { 
  val Configuration = ConfigFactory.load("lsr").withFallback(ConfigFactory.load())
  import language.implicitConversions

  implicit def config2RicherConfig(conf :Config) = new RicherConfig(conf)
}


