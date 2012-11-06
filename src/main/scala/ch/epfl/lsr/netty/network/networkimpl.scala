package ch.epfl.lsr.netty.network

import ch.epfl.lsr.netty.bootstrap._
import ch.epfl.lsr.netty.channel._
import ch.epfl.lsr.netty.codec.kryo._
import ch.epfl.lsr.netty.util.{ ChannelFutures }
import ch.epfl.lsr.protocol.{ ProtocolLocation => ProtocolLocationBase, Network => Network }

import org.jboss.netty.channel._

import java.util.concurrent.{ TimeUnit }
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.HashMap

import java.net.{ SocketAddress, InetSocketAddress, URI }

case class ProtocolLocation(str :String) extends ProtocolLocationBase { 
  val scheme = "lsr"

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

object NetworkingSystem { 
  private var networks = new AtomicReference(collection.immutable.HashMap.empty[ProtocolLocationBase, AbstractNetwork])

  def register(loc :ProtocolLocation, net :AbstractNetwork) { 
    val oldmap = networks.get
    if(! networks.compareAndSet(oldmap, oldmap.updated(loc, net))) { 
       println("retrying to register")
       register(loc, net)
    }
  }
  
  def isLocal(loc :ProtocolLocationBase) = { 
    networks.get.contains(loc)
  }

  import ch.epfl.lsr.netty.config._
  lazy val defaultOptions = Configuration.getMap("network")
  // ensure reading uses latest version
  @volatile
  private var systems  = collection.immutable.HashMap.empty[InetSocketAddress, NetworkingSystem]
  private val syslock = new Object()

  def getSystem(addr :InetSocketAddress, options :Map[String,Any] = null) :NetworkingSystem = { 
    val opts = if(options == null) defaultOptions else options
    systems.get(addr) match { 
      case Some(s) => s
      case None =>
	syslock.synchronized { // lock for writers, ensuring only one networksystem is created for each SocketAddr
	  systems.getOrElse(addr, {  
	    val sys = new NetworkingSystem(addr, opts)
	    systems = systems.updated(addr, sys)
	    sys
	  })
	}
    }
  }
}

class NetworkingSystem(val localAddress :InetSocketAddress, options :Map[String,Any]) { 
  import scala.collection.JavaConversions._

  def this(options :Map[String, Any]) { 
    this(options.get("localAddress").asInstanceOf[InetSocketAddress],options)
  }

  val dispatchingMap = new HashMap[String, AbstractNetwork]()

  def unbind(network :AbstractNetwork) = 
    dispatchingMap.synchronized { dispatchingMap remove network.name }

  def bind(network :AbstractNetwork) :NetworkingSystem = bind(network, network.name)

  def bind(network :AbstractNetwork, name :String) :NetworkingSystem = { 
    if(network == null) { 
      throw new NullPointerException("network")
    }

    NetworkingSystem.register(network.localId, network)
    dispatchingMap.synchronized { dispatchingMap.update(name, network) }
    this
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


abstract class AbstractNetwork(val localId: ProtocolLocation) extends Network { 
  def name = localId.name

  private val system : NetworkingSystem = NetworkingSystem.getSystem(localId.getSocketAddress).bind(this)

  override def close() { 
    super.close
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
    }
  }

  def sendTo(m :Any, ids :ProtocolLocationBase*) :Unit = { 
    ids.foreach{ 
      remoteId => 
	getOrCreateSource(remoteId.asInstanceOf[ProtocolLocation]).write(m)
    }
    ()
  }

  // creates a new Pipeline (used by system on connect to/from remote)
  def newPipeline =
    pipeline(
      new ChannelSource(localId, addSource _, onMessageReceived _) 
    )
}

