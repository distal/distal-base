package ch.epfl.lsr.netty.network

import _root_.ch.epfl.lsr.netty.bootstrap._
import _root_.ch.epfl.lsr.netty.channel._
import _root_.ch.epfl.lsr.netty.codec.kryo._
import _root_.ch.epfl.lsr.netty.util.{ ChannelFutures }
import _root_.ch.epfl.lsr.netty.util.Timer._
import _root_.ch.epfl.lsr.netty.protocol.{ ProtocolLocation }

import org.jboss.netty.channel._

import java.util.concurrent.{ TimeUnit }
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.HashMap

import java.net.{ SocketAddress, InetSocketAddress }

object ImplicitConversions { 
  import ch.epfl.lsr.netty.protocol.ImplicitConversions._
  //implicit def ProtocolLocation2SocketAddress(id :ProtocolLocation) :SocketAddress = id.getSocketAddress
}

object NetworkingSystem { 
  private var systems = new AtomicReference(collection.immutable.HashMap.empty[ProtocolLocation, AbstractNetwork])

  def register(loc :ProtocolLocation, net :AbstractNetwork) { 
    val oldsys = systems.get
    if(! systems.compareAndSet(oldsys, oldsys.updated(loc, net))) { 
       println("retrying to register")
       register(loc, net)
    }
  }
  
  def isLocal(loc :ProtocolLocation) = { 
    systems.get.contains(loc)
  }

  def sendLocal(m: Any, to :ProtocolLocation, from :ProtocolLocation) { 
    systems.get.apply(to).onMessageReceived(m, from)
  }
}

class NetworkingSystem(val localAddress :InetSocketAddress, options :Map[String,Any]) { 
  import scala.collection.JavaConversions._
  import ImplicitConversions._

  def this(options :Map[String, Any]) { 
    this(options.get("localAddress").asInstanceOf[InetSocketAddress],options)
  }

  val dispatchingMap = new HashMap[String, AbstractNetwork]()

  def unbind(network :AbstractNetwork) = 
    dispatchingMap.synchronized { dispatchingMap remove network.name }

  def bind(network :AbstractNetwork) :AbstractNetwork = bind(network, network.name)

  def bind(network :AbstractNetwork, name :String) :AbstractNetwork = { 
    if(network == null) { 
      throw new NullPointerException("network")
    }

    NetworkingSystem.register(network.localId, network)
    dispatchingMap.synchronized { dispatchingMap.update(name, network) }
    network
  }

  def sendLocal(m :Any, from :ProtocolLocation, remoteIds :ProtocolLocation*) { 
    remoteIds.foreach { 
      remoteId =>
	val network = dispatchingMap.synchronized { dispatchingMap.get(remoteId.name) }
	assume(network.nonEmpty, "network for "+remoteId+" not found: "+dispatchingMap.synchronized { dispatchingMap.keys })
        network.get.onMessageReceived(m, from)
    }
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
	//new PrintWrittenHandler{ },
	//new PrintingHandler{ },
	new DispatchingHandler(getPipelineExtension _),
	// everyone uses kryo, so we can already add that
	new KryoEncoder(),
	new KryoDecoder()
      )
    }
    //println("binding "+localAddress)
    bs bind localAddress
    bs
  }


  val remoteSelector = new RemoteSelectionHandler()
  val reconnector = new ReconnectionHandler(100, copyPipeline)
  val clientBootstrap = SocketClient.bootstrap(options) { newClientPipeline }
  private def newClientPipeline = { 
    pipeline (
      //new PrintWrittenHandler{ },
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
  
  def connectTo(other :ProtocolLocation, localNetwork :AbstractNetwork, source :ChannelSource) :ChannelFuture = { 
    
    val future = clientBootstrap.bind(localAddress) 
    val pipeline = future.getChannel.getPipeline
    pipeline.addLast("--source", source)

    RemoteSelectionHandler.setSelectionString(pipeline, other.name)
    
    source.connect(other)
  }
}


trait Network { 
  def sendTo(m :Any, ids :ProtocolLocation*)
  def forwardTo(m :Any, to :ProtocolLocation, from :ProtocolLocation) 
  def close
  
  
  def onMessageReceived(msg :Any, from :ProtocolLocation)
}

abstract class AbstractNetwork(val localId: ProtocolLocation) extends Network { 
  def name = localId.name
  @volatile
  var system : NetworkingSystem = _

  def bindTo(toBind :NetworkingSystem) { 
    if(system != null) 
      throw new Exception("already bound")
    system = toBind
    system.bind(this)
  }

  def close() { 
    system.unbind(this)
    sources.synchronized{ 
      sources.mapValues{ _.close }
    }
  }

  private val sources = new HashMap[ProtocolLocation,ChannelSource]()

  private def addSource(id :ProtocolLocation, source :ChannelSource) { 
    sources.synchronized{ 
      // URL strangeness
      // sources.keySet.map { 
      // 	k =>
      // 	  if(k.toString != id.toString) { 
      // 	    println("EQ?"+(k==id)+" "+(k.hashCode == id.hashCode)+" "+(k.uri == id.uri)+k.toString+" "+id.toString)
      // 	    assume(k.hashCode != id.hashCode)
      // 	  }
      // }
      //println("adding "+id.hashCode+" "+id+" "+source.hashCode+" "+sources.keySet)
      sources.update(id, source) 
    }
  }
  private def getSource(id :ProtocolLocation) = { 
    sources.synchronized{ 
      sources.get(id) 
    }
  }

  private def getOrCreateSource(id :ProtocolLocation) :ChannelSource = { 
    sources.synchronized{ 
      sources.getOrElseUpdate(id, {
	val source = new ChannelSource(localId, addSource _, onMessageReceived _) 
	system.connectTo(id, this, source)
	source
      })
      // var rv = sources.get(id)
      // if(rv.nonEmpty) { 
      // 	rv.get
      // } else { 
      // 	val source = new ChannelSource(localId, addSource _, onMessageReceived _) 
      // 	system.connectTo(id, this, source)
      // 	sources.update(id, source)
      // 	source
      // }
    }
  }

  def forwardTo(m :Any, to :ProtocolLocation, from :ProtocolLocation) = { 
    assume(NetworkingSystem.isLocal(to), "forwarding is only supported for local protocols")
    NetworkingSystem.sendLocal(m, to, from)
  }

  def sendTo(m :Any, ids :ProtocolLocation*) :Unit = { 
    ids.foreach{ 
      remoteId => 
	if(NetworkingSystem.isLocal(remoteId)) { 
	  NetworkingSystem.sendLocal(m, localId, remoteId)
	} else { 
	  getOrCreateSource(remoteId).write(m)
	}
    }
    ()
  }

  def onMessageReceived(e :Any, from :ProtocolLocation) 

  // creates a new Pipeline (used by system on connect to/from remote)
  def newPipeline =
    pipeline(
      new ChannelSource(localId, addSource _, onMessageReceived _) 
    )
}

