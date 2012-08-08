package ch.epfl.lsr.netty.protocol

import ch.epfl.lsr.netty.network.{ AbstractNetwork, Network, NetworkingSystem }
import ch.epfl.lsr.netty.channel.ChannelSource
import ch.epfl.lsr.netty.execution.InProtocolPool

import ch.epfl.lsr.netty.config._
import com.typesafe.config._

import scala.collection.mutable.HashMap

import java.net.{ InetSocketAddress }

object implicitConversions { 
  implicit def ProtocolLocation2SocketAddress(id :ProtocolLocation) :InetSocketAddress = id.getSocketAddress
}


case class ProtocolLocation(name :String, host :String, port :Int) { 
  def this(u :java.net.URI) = this(u.getPath, u.getHost, u.getPort)
  lazy val getSocketAddress = new InetSocketAddress(host, port)
}


trait ProtocolRunnable extends Runnable { 
  def protocol : Protocol
}

abstract class AbstractProtocolRunnable(val protocol :Protocol) extends ProtocolRunnable { 
}

class DefaultProtocolRunnable(protocol :Protocol, runnable: Runnable) extends AbstractProtocolRunnable(protocol) { 
  def run = runnable.run
}


trait Protocol {
  lazy val network :Network = Protocol.getBoundNetwork(this)
  def location :ProtocolLocation

  final def start = { 
    network; 
    onStart
  }

  def inPool(task : =>Unit) { 
    InProtocolPool.execute(this, task)
  }

  def fireMessageReceived(m :AnyRef) { 
    inPool(onMessageReceived(m))
  }

  def onMessageReceived(m :AnyRef) :Unit
  def onStart :Unit = { }
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
	config = Configuration.getMap("network")
      config
    }
  }

  private def getSystem(localAddress :InetSocketAddress) :NetworkingSystem = {
    map.synchronized { 
      map.get(localAddress) match { 
	case Some(system) => system
	case None => 
	  val newSystem = new NetworkingSystem(localAddress, getConfig)
	  map.update(localAddress, newSystem)
	  newSystem
      }
    }
  }

  private class ProtocolNetwork(protocol: Protocol) extends AbstractNetwork(protocol.location) { 
    def onMessageReceived(ctx :ChannelSource, msg :AnyRef) { 
      // execute the handler in ProtocolPool
      protocol.fireMessageReceived(msg)
    }
  }

  private def getBoundNetwork(protocol :Protocol) = { 
    val network = new ProtocolNetwork(protocol)
    network.bindTo(getSystem(protocol.location))
    network
  }
}

