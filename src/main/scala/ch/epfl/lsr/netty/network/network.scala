package ch.epfl.lsr.netty.network

import _root_.ch.epfl.lsr.netty.bootstrap._
import _root_.ch.epfl.lsr.netty.channel._
import _root_.ch.epfl.lsr.netty.codec.kryo._
import _root_.ch.epfl.lsr.netty.util.{ ChannelFutures }
import _root_.ch.epfl.lsr.netty.util.Timer._
import _root_.ch.epfl.lsr.netty.execution.InDownPool
import _root_.ch.epfl.lsr.netty.protocol.{ ProtocolLocation }

import org.jboss.netty.channel._

import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashMap

import java.net.{ SocketAddress, InetSocketAddress }

object implicitConversions { 
  import _root_.ch.epfl.lsr.netty.protocol.implicitConversions._
  implicit def ProtocolLocation2SocketAddress(id :ProtocolLocation) :SocketAddress = id.getSocketAddress
}


class NetworkingSystem(val localAddress :InetSocketAddress, options :Map[String,Any]) { 
  import scala.collection.JavaConversions._
  import implicitConversions._

  def this(options :Map[String, Any]) { 
    this(options.get("localAddress").asInstanceOf[InetSocketAddress],options)
  }

  val dispatchingMap = new HashMap[String, AbstractNetwork]()

  def bind(network :AbstractNetwork) :AbstractNetwork = bind(network, network.name)

  def bind(network :AbstractNetwork, name :String) :AbstractNetwork = { 
    if(network == null) { 
      throw new NullPointerException("network")
    }
    println("binding network to "+name)
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


  val remoteSelector = new RemoteSelectionHandler()
  val reconnector = new ReconnectionHandler(100, copyPipeline)
  val clientBootstrap = SocketClient.bootstrap(options) { newClientPipeline }
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

  def connectTo(other :ProtocolLocation, localNetwork :AbstractNetwork, toAppend :ChannelPipeline) :ChannelFuture = { 

    println("connecting to "+other+" ")
    //new Exception("").printStackTrace
    
    val future = clientBootstrap.bind(localAddress) 
    val channel = future.getChannel
    val pipeline = channel.getPipeline

    for(e <- toAppend.toMap.entrySet) { 
      pipeline.addLast(localNetwork.toString+"--"+e.getKey, e.getValue)
    }

    RemoteSelectionHandler.setSelectionString(channel.getPipeline,other.name)

    channel.connect(other)
  }
}


trait Network { 
  def sendTo(m :Any, ids :ProtocolLocation*)
}

abstract class AbstractNetwork(val localId: ProtocolLocation) extends Network { 
  def name = localId.name
  var system : NetworkingSystem = _

  def bindTo(toBind :NetworkingSystem) { 
    if(system != null) 
      throw new Exception("already bound")
    system = toBind
    system.bind(this)
  }

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

  def onMessageReceived(ctx :ChannelSource, e :AnyRef) 

  // creates a new Pipeline (used by system on connect to/from remote)
  def newPipeline = {
    pipeline(
      new MessageReceivedHandler with ChannelSource { 
	override def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent) { 
	  if(e.getMessage.isInstanceOf[ProtocolLocation]) { 
	    // for the server side
	    addSource(e.getMessage.asInstanceOf[ProtocolLocation], this)
	  } else { 
	    onMessageReceived(this, e.getMessage)
	  }
	}
      }
      )
    }
}

