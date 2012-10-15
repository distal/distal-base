package ch.epfl.lsr.netty.protocol

import ch.epfl.lsr.netty.network.{ AbstractNetwork, Network, NetworkingSystem }
import ch.epfl.lsr.netty.channel.ChannelSource
import ch.epfl.lsr.netty.execution.InProtocolPool

import ch.epfl.lsr.netty.config._
import com.typesafe.config._

import scala.collection.mutable.HashMap

import java.net.{ InetSocketAddress, URI }

object ImplicitConversions { 
  // class InetSocketAddressWithPath(addr :InetSocketAddress) { 
  //   def /(s :String) = { new ProtocolLocation(if(s startsWith "/") s else ("/"+s), addr) }}

//  import language.implicitConversions
//  implicit def InetSocketAddress2InetWithPath(addr :InetSocketAddress) :InetSocketAddressWithPath = new InetSocketAddressWithPath(addr)
}


case class ProtocolLocation(str :String) { 
  val uri = new URI(str)
//  def this(u :URI) = this(u.toString)

  def name :String = uri.getPath
  def host :String = uri.getHost
  def port :Int = OrDefaultPort(uri.getPort)
  lazy val clazz :Option[Class[_]] = ClassOrNone(uri.getUserInfo)

  def isForClazz(c :Class[_]) = clazz.filter{ _ == c}.nonEmpty
  lazy val getSocketAddress = new InetSocketAddress(host, port)
  def /(s :String) = { 
    ProtocolLocation(uri.toString+s)
  }

  private def OrDefaultPort(port :Int) = if(port == -1) 2552 else port
  private def ClassOrNone(s :String) :Option[Class[_]] = if(s==null) None else Some(Class.forName(s))
}


trait ProtocolRunnable extends Runnable { 
  def protocol : Protocol
}

abstract class AbstractProtocolRunnable(val protocol :Protocol) extends ProtocolRunnable { 
}

class DefaultProtocolRunnable(protocol :Protocol, runnable: Runnable) extends AbstractProtocolRunnable(protocol) { 
  def run = runnable.run
}

class AlreadyShutdownException extends Exception

trait Protocol {
  @volatile
  private var _isShutdown = false
  private lazy val _theNetwork :Network = Protocol.getBoundNetwork(this)
  
  def network : Network = _theNetwork
  def location :ProtocolLocation

  def getConfig :Option[Config] = None

  final def start = { 
    network; 
    inPool { 
      if(_isShutdown)
	throw new AlreadyShutdownException
    	afterStart
    }
  }

  final def shutdown = { 
    beforeShutdown
    _isShutdown = true
    InProtocolPool.unregister(this)
    network.close
  }

  def inPool(task : =>Unit) { 
    if(_isShutdown)
      throw new AlreadyShutdownException
    InProtocolPool.execute(this, task)
  }

  def isShutdown = _isShutdown

  def fireMessageReceived(m :Any, remoteLocation :ProtocolLocation) { 
    if(_isShutdown)
      throw new AlreadyShutdownException
    if(remoteLocation == null) { 
      println("null")
      throw new Exception("remoteLocation == null")
    }
    inPool(onMessageReceived(m, remoteLocation))
  }

  def onMessageReceived(m :Any, remoteLocation :ProtocolLocation) :Unit
  def afterStart :Unit = { }
  def beforeShutdown :Unit = { }
}

object Protocol { 
  import ImplicitConversions._
  
  private val map = HashMap.empty[InetSocketAddress, NetworkingSystem]
  private val lock = new Object
  private var config :Map[String, AnyRef] = null
  
  def setConfig(newConfig :Config) { 
    val asMap = newConfig.toMap
    lock.synchronized { 
      config = asMap
    }
  }

  def getConfig() = { 
    lock.synchronized { 
      if(config==null)
	config = Configuration.getMap("network")
      config
    }
  }

  private def getSystem(addr :InetSocketAddress) = 
    map.synchronized { map.get(addr) }

  private def getSystemOrElseCreate(localAddress :InetSocketAddress, config :Option[Config] = None) :NetworkingSystem = {
    var conf = 
      if(config.nonEmpty) config.get.getMap("network") else null
    if(conf == null)
      conf = getConfig

    map.synchronized { 
      map.get(localAddress) match { 
	case Some(system) => system
	case None => 
	  val newSystem = new NetworkingSystem(localAddress, conf)
	  map.update(localAddress, newSystem)
	  newSystem
      }
    }
  }

  private class ProtocolNetwork(protocol: Protocol) extends AbstractNetwork(protocol.location) { 

    def onMessageReceived(msg :Any, from :ProtocolLocation) { 

      // execute the handler in ProtocolPool
      protocol.fireMessageReceived(msg, from)
    }

    // Protocol object knows about locally created ones
    override def sendTo(m :Any, ids :ProtocolLocation*) { 

      val bySystem = ids.groupBy { id :ProtocolLocation => Protocol.getSystem(id.getSocketAddress) }
      
      bySystem.foreach { 
	case (Some(system),locals) =>
	  system.sendLocal(m, localId, locals :_*)
	case (None,remotes) =>
	  super.sendTo(m, remotes :_*)
      }
    }
  }

  private def getBoundNetwork(protocol :Protocol) = { 
    val network = new ProtocolNetwork(protocol)
    network.bindTo(getSystemOrElseCreate(protocol.location.getSocketAddress,protocol.getConfig))
    network
  }
}

