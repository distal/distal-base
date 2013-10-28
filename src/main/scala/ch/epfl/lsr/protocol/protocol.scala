package ch.epfl.lsr.protocol

import ch.epfl.lsr.util.execution.InProtocolPool

import scala.collection.mutable.HashMap

import java.net.{ InetSocketAddress, URI }

trait ProtocolLocation { 
  def scheme :String
  def /(path :String) :ProtocolLocation
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
  private lazy val network :Network = NetworkFactory.createNetwork(location, this)
  def location :ProtocolLocation

  def sendTo(m :Any, ids :ProtocolLocation*) { 
    val bySystem = ids.groupBy { NetworkFactory.getLocal(_) }
      
    bySystem.foreach { 
      case (Some(network),locals) =>
	locals.foreach { 
	  local => 
	    network.onMessageReceived(m, location)
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

  def inPoolNext(task : =>Unit) { 
    if(_isShutdown)
      throw new AlreadyShutdownException
    InProtocolPool.executeNext(this, task)
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

import ch.epfl.lsr.netty.network.{ ProtocolLocation => DefaultProtocolLocation}
import ch.epfl.lsr.netty.network.{ AbstractNetwork => NettyNetwork }

trait ProtocolNetwork extends Network { 
  val protocol :Protocol

  def onMessageReceived(msg :Any, from :ProtocolLocation) { 
    // execute the handler in ProtocolPool
    protocol.fireMessageReceived(msg, from)
  }
}


abstract class NettyBasedProtocolNetwork(location :DefaultProtocolLocation, val protocol :Protocol) extends NettyNetwork(location) with ProtocolNetwork { 
  def getPipeline :org.jboss.netty.channel.ChannelPipeline
}

class DefaultProtocolNetwork(location :DefaultProtocolLocation, protocol: Protocol) extends NettyBasedProtocolNetwork(location, protocol) { 
  import ch.epfl.lsr.netty.bootstrap.pipeline
  import ch.epfl.lsr.netty.codec.kryo._
  
  def getPipeline = pipeline( 
    new KryoEncoder(),
    new KryoDecoder()
  )
}

class DefaultFactory extends NetworkFactory { 
  def createNetwork(location :ProtocolLocation, protocol :Protocol) :Network =
    new DefaultProtocolNetwork(location.asInstanceOf[DefaultProtocolLocation], protocol)
}


