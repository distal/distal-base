package ch.epfl.lsr.netty.protocol

import ch.epfl.lsr.netty.network.{ AbstractNetwork, Network, NetworkingSystem }
import ch.epfl.lsr.netty.channel.ChannelSource
import ch.epfl.lsr.netty.execution.InProtocolPool

import ch.epfl.lsr.netty.config._
import com.typesafe.config._

import scala.collection.mutable.HashMap

import java.net.{ InetSocketAddress }

object implicitConversions { 
  class InetSocketAddressWithPath(addr :InetSocketAddress) { 
    def /(s :String) = { new ProtocolLocation(if(s startsWith "/") s else ("/"+s), addr) }}
  implicit def ProtocolLocation2SocketAddress(id :ProtocolLocation) :InetSocketAddress = id.getSocketAddress
  implicit def InetSocketAddress2InetWithPath(addr :InetSocketAddress) :InetSocketAddressWithPath = new InetSocketAddressWithPath(addr)
}

case class ProtocolLocation(name :String, host :String, port :Int) { 
  def this(u :java.net.URI) = this(u.getPath, u.getHost, u.getPort)
  def this(name :String, s :InetSocketAddress) = this(name, s.getHostName, s.getPort)
  lazy val getSocketAddress = new InetSocketAddress(host, port)
  def /(s :String) = { 
    val postfix = if(s startsWith "/") s else "/"+s
    new ProtocolLocation(name+postfix, host, port)
  }
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
  private var isShutdown = false
  lazy val theNetwork :Network = Protocol.getBoundNetwork(this)
  
  def network : Network = theNetwork
  def location :ProtocolLocation

  def getConfig :Option[Config] = None

  final def start = { 
    if(isShutdown)
      throw new AlreadyShutdownException
    network; 
    afterStart
  }

  final def shutdown = { 
    beforeShutdown
    isShutdown = true
    InProtocolPool.unregister(this)
    network.close
  }

  def inPool(task : =>Unit) { 
    if(isShutdown)
      throw new AlreadyShutdownException
    InProtocolPool.execute(this, task)
  }

  def fireMessageReceived(m :Any, remoteLocation :ProtocolLocation) { 
    if(isShutdown)
      throw new AlreadyShutdownException
    inPool(onMessageReceived(m, remoteLocation))
  }

  def onMessageReceived(m :Any, remoteLocation :ProtocolLocation) :Unit
  def afterStart :Unit = { }
  def beforeShutdown :Unit = { }
}

object Protocol { 
  import implicitConversions._
  
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
	config = Configuration.getMap("networx")
      config
    }
  }

  private def getSystem(addr :InetSocketAddress) = 
    map.synchronized { map.get(addr) }

  private def getSystemOrElseCreate(localAddress :InetSocketAddress, config :Option[Config]) :NetworkingSystem = {
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
    network.bindTo(getSystemOrElseCreate(protocol.location,protocol.getConfig))
    network
  }
}

