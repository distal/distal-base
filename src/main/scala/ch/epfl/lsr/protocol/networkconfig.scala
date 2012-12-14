package ch.epfl.lsr.protocol

import ch.epfl.lsr.config._

object NetworkConfig { 
  private val conf = Configuration.getConfig("ch.epfl.lsr.protocol") 
  private val defaults = conf.getConfig("default")
  private val schemas = conf.getConfig("schemas")
  
  def configuredSchemas :List[String] = { 
    import scala.collection.JavaConversions._
    
    schemas.root.keySet.toList
  } 

  def apply(scheme :String) = { 
    schemas.getConfig(scheme).withFallback(defaults)
  }

  def asMap(schema :String) = { 
    apply(schema).toMap
  }

}
