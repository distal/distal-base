package ch.epfl.lsr.netty.config

import java.net.URI

import com.typesafe.config._

class RicherConfig(config :Config) { 
  import scala.collection.JavaConverters._
  
  def toMap = config.entrySet.asScala.map{ e => (e.getKey,e.getValue) }.toMap
  def getMap(path :String) :Map[String,Object] = try { config.getObject(path).unwrapped.asScala.toMap } catch { case _ => Map.empty }
  def getClazz[T](path :String) :Class[T] = Class.forName(config.getString(path)).asInstanceOf[Class[T]]
  def getURI(path :String) :URI = new URI(config.getString(path))
}

