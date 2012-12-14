package ch.epfl.lsr

import com.typesafe.config.{ Config, ConfigFactory }

package object config { 
  lazy val Configuration = { 
    val lsr = ConfigFactory.parseResources("lsr")
    if(lsr.isEmpty)
      ConfigFactory.load()
    else
      lsr
  }
  
  import language.implicitConversions

  implicit def config2RicherConfig(conf :Config) = new RicherConfig(conf)
}


