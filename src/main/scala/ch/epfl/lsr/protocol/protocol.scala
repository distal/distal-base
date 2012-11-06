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
  private lazy val network :Network = NetworkFactory.newNetwork(location, this)
  def location :ProtocolLocation

  def sendTo(m :Any, ids :ProtocolLocation*) { 
    val bySystem = ids.groupBy { NetworkFactory.getLocal(_) }
      
    bySystem.foreach { 
      case (Some(network),locals) =>
	locals.foreach { 
	  local => 
	    network.onMessageReceived(m, local)
	}
      case (None,remotes) =>
	network.sendTo(m, remotes :_*)
    }
  }

  def forwardTo(m :Any, to :ProtocolLocation, from :ProtocolLocation) = { 
    NetworkFactory.getLocal(to) match { 
      case Some(network) =>
	network.onMessageReceived(m, from)
      case None =>
	throw new Exception("cannot forward to remote protocols :"+to)
    }
  }

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

  final def isShutdown = _isShutdown

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

  import ch.epfl.lsr.netty.network.{ ProtocolLocation => DefaultProtocolLocation}
  private class DefaultProtocolNetwork(location :DefaultProtocolLocation, protocol: Protocol) extends AbstractNetwork(location) { 

    def onMessageReceived(msg :Any, from :ProtocolLocation) { 
      // execute the handler in ProtocolPool
      protocol.fireMessageReceived(msg, from)
    }
  }    

  val defaultCreator :NetworkFactory.Creator = { 
    (location,protocol) => 
      new DefaultProtocolNetwork(location.asInstanceOf[DefaultProtocolLocation], protocol)
  }

  def registerDefault = NetworkFactory.registerScheme("lsr", defaultCreator)
}

