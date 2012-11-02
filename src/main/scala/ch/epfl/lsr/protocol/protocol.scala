package ch.epfl.lsr.protocol

import ch.epfl.lsr.netty.network.{ AbstractNetwork, NetworkingSystem }
import ch.epfl.lsr.util.execution.InProtocolPool

import ch.epfl.lsr.netty.config._
import com.typesafe.config._

import scala.collection.mutable.HashMap

import java.net.{ InetSocketAddress, URI }

trait ProtocolLocation { 
  def scheme :String
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
  private lazy val _theNetwork :Network = NetworkFactory.newNetwork(location, this)
  
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


  // TODO move this ?
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

  import ch.epfl.lsr.netty.network.{ ProtocolLocation => DefaultProtocolLocation}

  private class DefaultProtocolNetwork(location :DefaultProtocolLocation, protocol: Protocol) extends AbstractNetwork(location) { 

    def onMessageReceived(msg :Any, from :ProtocolLocation) { 

      // execute the handler in ProtocolPool
      protocol.fireMessageReceived(msg, from)
    }

    // Protocol object knows about locally created ones
    override def sendTo(m :Any, ids :ProtocolLocation*) { 
      val locations = ids.asInstanceOf[Seq[DefaultProtocolLocation]]

      val bySystem = locations.groupBy { loc :DefaultProtocolLocation => Protocol.getSystem(loc.getSocketAddress) }
      
      bySystem.foreach { 
	case (Some(system),locals) =>
	  system.sendLocal(m, localId, locals :_*)
	case (None,remotes) =>
	  super.sendTo(m, remotes :_*)
      }
    }
  }

  val defaultCreator :NetworkFactory.Creator = { 
    (location,protocol) => 
      new DefaultProtocolNetwork(location.asInstanceOf[DefaultProtocolLocation], protocol)
  }

  NetworkFactory.registerScheme("lsr", defaultCreator)
}

