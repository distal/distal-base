package ch.epfl.lsr.netty.network.akka

import ch.epfl.lsr.netty.bootstrap._
import ch.epfl.lsr.netty.channel._
import ch.epfl.lsr.netty.codec.kryo._
import ch.epfl.lsr.netty.util.{ InDownPool, ChannelFutures }
import ch.epfl.lsr.netty.util.Timer._

import org.jboss.netty.channel._

import java.util.concurrent.TimeUnit

import _root_.akka.actor._

import scala.collection.mutable.HashMap

import java.net.{ SocketAddress, InetSocketAddress }

case class ProtocolLocation(name :String, host :String, port :Int) { 
  def this(u :java.net.URI) = this(u.getPath, u.getHost, u.getPort)
  lazy val getSocketAddress = new InetSocketAddress(host, port)
}


class NetworkingSystem(val hostname:String, val port:Int, options :Map[String,Any]) { 
  import scala.collection.JavaConversions._
  import implicitConversions._

  val localAddress = new InetSocketAddress(hostname, port)

  def this(addr :InetSocketAddress, options :Map[String, Any]) = { 
    this(addr.getHostName, addr.getPort, options.toMap)
  }

  def this(options :Map[String, Any]) { 
    this(options.get("localAddress").asInstanceOf[InetSocketAddress],options)
  }

  def this(options :Tuple2[String, Any]*) { 
    this(options.toMap)
  }

  val dispatchingMap = new HashMap[String, ActorNetwork]()

  def bind(actor :ActorRef, name: String) = { 
    if(actor == null)
      throw new Exception("null Actor")
    
    println("binding actor to "+name)
    val network = new ActorNetwork(actor, name, this)
    dispatchingMap.synchronized { dispatchingMap.update(name, network) }
    network
  }

  def getPipelineExtension(name :String) : Option[ChannelPipeline] = { 
    val network = dispatchingMap.synchronized {  dispatchingMap.get(name) }

    if(network.isEmpty) { 
      None
    } else { 
      Some(network.get.newPipeline)
    }
  }

  val serverBootstrap = { 
    val bs = SocketServer.bootstrap(options) { 
      pipeline (
	//new PrintingHandler{ },
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
      //new PrintingHandler{ },
      reconnector,
      remoteSelector,
      // everyone uses kryo, so we can already add that
      new KryoEncoder(),
      new KryoDecoder()
    )
  }

  private def copyPipeline(oldpipe :ChannelPipeline) :ChannelPipeline = { 
    val newpipe = pipeline()
    
    for(e <- oldpipe.toMap.entrySet) { 
      newpipe.addLast(e.getKey, e.getValue)
    }

    RemoteSelectionHandler.copySelectionString(oldpipe, newpipe)
    newpipe
  }

  val remoteSelector = new RemoteSelectionHandler()
  val reconnector = new ReconnectionHandler(100, copyPipeline)
  val clientBootstrap = { 
    SocketClient.bootstrap(options) { newClientPipeline }
  }

  def connectTo(other :ProtocolLocation, localNetwork :ProtocolNetwork, toAppend :ChannelPipeline) :ChannelFuture = { 

    println("connecting to "+other+" ")
    //new Exception("").printStackTrace
    
    val future = clientBootstrap.bind(new InetSocketAddress(port)) 
    val channel = future.getChannel
    val pipeline = channel.getPipeline

    for(e <- toAppend.toMap.entrySet) { 
      pipeline.addLast(localNetwork.toString+"--"+e.getKey, e.getValue)
    }

    RemoteSelectionHandler.setSelectionString(channel.getPipeline,other.name)

    channel.connect(other)
  }
}

abstract class ProtocolNetwork(name :String, system :NetworkingSystem) { 
  val localId = new ProtocolLocation(name, system.hostname, system.port)

  private val sources = new HashMap[ProtocolLocation,ChannelSource]()

  private def addSource(id :ProtocolLocation, source :ChannelSource) { 
    sources.synchronized{ sources.update(id, source) }
  }
  private def getSource(id :ProtocolLocation) = { 
    sources.synchronized{ sources.get(id) }
  }

  def sendTo(m :Any, ids :ProtocolLocation*) :Unit = { 
    ids.foreach{ 
      remoteId => 
	val src = getSource(remoteId)
	if(src.nonEmpty) { 
	  InDownPool.write(src.get, m) 
	} else { 
	  val pipeline = newPipeline
	  val source = pipeline.getLast.asInstanceOf[ChannelSource]
	  pipeline.addFirst("oneShotSender", new OneShotOnConnectHandler({ 
	    (ctx :ChannelHandlerContext, e :ChannelStateEvent) => 
	      println("exec handler")
	      InDownPool.write(source, localId) /* tell other side who we are */
	      InDownPool.write(source, m)
	  }))
	  addSource(remoteId, source)
	  system.connectTo(remoteId, this, pipeline)
	}
    }
    ()
  }

  def onMessageReceived(ctx :ChannelHandlerContext, e :MessageEvent) 

  // creates a new Handler (used by system on connect to/from remote)
  def newPipeline = {
    pipeline(
      new MessageReceivedHandler with ChannelSource { 
	override def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent) { 
	  println("received "+e.getMessage)
	  if(e.getMessage.isInstanceOf[ProtocolLocation]) { 
	    // for the server side
	    addSource(e.getMessage.asInstanceOf[ProtocolLocation], this)
	  } else { 
	    onMessageReceived(ctx, e)
	  }
	}
      }
      )
    }
}


class ActorNetwork(actor :ActorRef, name: String, system :NetworkingSystem) extends ProtocolNetwork(name, system){ 
  
  def onMessageReceived(ctx :ChannelHandlerContext, e :MessageEvent) { 
    actor ! e.getMessage
  }
}


