package ch.epfl.lsr.netty.network.akka

import ch.epfl.lsr.netty.bootstrap._
import ch.epfl.lsr.netty.channel.{ DispatchingHandler, MessageReceivedHandler, RemoteSelectionHandler, ReconnectionHandler }
import ch.epfl.lsr.netty.channel.akka.ActorForwardingHandler
import ch.epfl.lsr.netty.codec.kryo._
import ch.epfl.lsr.netty.util.{ InIOThread, ChannelFutures }

import org.jboss.netty.channel._


import akka.actor._

import scala.collection.mutable.HashMap

import java.net.{ SocketAddress, InetSocketAddress }

object implicitConversions { 
  implicit def ActorConnectionId2SocketAddress(id :ActorConnectionId) :SocketAddress = id.toSocketAddress

}

case class ActorConnectionId(name :String, host :String, port :Int) { 
  lazy val toSocketAddress = new InetSocketAddress(host, port)

}

class NetworkingSystem(val hostname:String, val port:Int, options :Tuple2[String,Any]*) { 
  import implicitConversions._

  val localAddress = new InetSocketAddress(hostname, port)

  def this(addr :InetSocketAddress, options :Tuple2[String, Any]*) = { 
    this(addr.getHostName, addr.getPort, options :_*)
  }

  def this(options :Tuple2[String, Any]*) { 
    this(options.toMap.get("localAddress").asInstanceOf[InetSocketAddress],options :_*)
  }

  val dispatchingMap = new HashMap[String, ActorNetwork]()

  def bind(actor :ActorRef, name: String) = { 
    if(actor == null)
      throw new Exception("null Actor")
    
    println("binding actor to "+name+" (this="+this+")")
    val network = new ActorNetwork(actor, name, this)
    dispatchingMap.synchronized { dispatchingMap.update(name, network) }
    network
  }

  def getPipelineExtension(name :String) : Option[ChannelPipeline] = { 
    val network = dispatchingMap.synchronized {  dispatchingMap.get(name) }

    if(network.isEmpty) { 
      None
    } else { 
      Some(pipeline (
	network.get.newHandler
      ))
    }
  }

  val serverBootstrap = { 
    val bs = NIOSocketServer.bootstrap(options :_*) { 
      pipeline (
	new DispatchingHandler(getPipelineExtension _),
	// everyone uses kryo, so we can already add that
	new KryoEncoder(),
	new KryoDecoder()
      )
    }
    println("binding "+localAddress)
    bs bind localAddress
    bs
  }

  private def newClientPipeline = { 
    pipeline (
      reconnector,
      remoteSelector,
      // everyone uses kryo, so we can already add that
      new KryoEncoder(),
      new KryoDecoder()
    )
  }

  private def copyPipeline(oldpipe :ChannelPipeline) :ChannelPipeline = { 
    val newpipe = newClientPipeline
    RemoteSelectionHandler.copySelectionString(oldpipe, newpipe)
    newpipe
  }


  val remoteSelector = new RemoteSelectionHandler()
  val reconnector = new ReconnectionHandler(50, copyPipeline)
  val clientBootstrap = { 
    NIOSocketCient.bootstrap(options :_*) { newClientPipeline }
  }

  def connectTo(other :ActorConnectionId, localNetwork :ActorNetwork) :ChannelFuture = { 

    println("connecting to "+other+" ")
    //new Exception("").printStackTrace
    
    val future = clientBootstrap.bind(new InetSocketAddress(port))
    val channel = future.getChannel
    val handler = localNetwork.newHandler
    RemoteSelectionHandler.setSelectionString(channel.getPipeline,other.name)
    channel.getPipeline.addLast("actor", handler)
    channel.connect(other)
  }
}


class ActorNetwork(actor :ActorRef, name: String, system :NetworkingSystem) { 
  val localId = new ActorConnectionId(name, system.hostname, system.port)

  private val contexts = new HashMap[ActorConnectionId,ChannelHandlerContext]()

  private def addContext(id :ActorConnectionId, ctx :ChannelHandlerContext) { 
    contexts.synchronized{ contexts.update(id, ctx) }
  }
  private def getContext(id :ActorConnectionId) = { 
    contexts.synchronized{ contexts.get(id) }
  }
  

  def sendTo(m :Any, ids :ActorConnectionId*) :Unit = { 
    ids.foreach{ 
      remoteId => 
	val ctx = getContext(remoteId)
	if(ctx.nonEmpty) { 
	  InIOThread.write(ctx.get, m)
	} else { 
	  val future = system.connectTo(remoteId, this)
	  ChannelFutures.onCompleted(future) { 
	    f => 
	      val pipe = f.getChannel.getPipeline
	      addContext(remoteId, pipe.getContext(pipe.getLast))
	      sendTo(localId, remoteId) /* tell other side who we are */
	      sendTo(m , remoteId)
	  }
	}
    }
    ()
  }

  // creates a new Handler (used by system on connect to/from remote)
  def newHandler = { 
    new MessageReceivedHandler { 
      override def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent) { 
	if(e.getMessage.isInstanceOf[ActorConnectionId]) { 
	  addContext(e.getMessage.asInstanceOf[ActorConnectionId], ctx)
	} else { 
	  actor ! e.getMessage
	}
      }
    }
  }
}
