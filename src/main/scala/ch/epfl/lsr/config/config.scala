package ch.epfl.lsr.config

import java.net.URI

import com.typesafe.config._

import scala.collection.immutable.Map


class RicherConfig(config :Config) { 
  import scala.collection.JavaConverters._
  
  def toMap = config.entrySet.asScala.map{ e => (e.getKey,e.getValue) }.toMap.withDefault { 
    key => 
      throw new Exception("no value configured for "+key)
  }
  def getMap(path :String) :Map[String,AnyRef] = { 
    try { 
      config.getObject(path).unwrapped.asScala.toMap
    } catch { 
      case _ :Exception => Map.empty 
    }
  }
  def getClazz[T](path :String) :Class[T] = Class.forName(config.getString(path)).asInstanceOf[Class[T]]
  def getURI(path :String) :URI = new URI(config.getString(path))
}

