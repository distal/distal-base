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
  val uri = new URI(str)
//  def this(u :URI) = this(u.toString)

  val scheme = uri.getScheme

  def name :String = uri.getPath
  def host :String = uri.getHost
  def port :Int = OrDefaultPort(uri.getPort)
  lazy val clazz :Option[Class[_]] = ClassOrNone(uri.getUserInfo)

  def isForClazz(c :Class[_]) = clazz.filter{ _ == c}.nonEmpty
  def getSocketAddress = new InetSocketAddress(host, port)
  def /(s :String) = { 
    ProtocolLocation(uri.toString+s)
  }

  private def OrDefaultPort(port :Int) = if(port == -1) 2552 else port
  private def ClassOrNone(s :String) :Option[Class[_]] = if(s==null) None else Some(Class.forName(s))

  def to(remote :ProtocolLocation) = ConnectionDescriptor(this, remote)
}

case class ConnectionDescriptor(local :ProtocolLocation, remote :ProtocolLocation) { 
  def reverse = ConnectionDescriptor(remote, local)
}

object ConnectionDescriptor { 
              // ConnectionDescriptor(ProtocolLocation(......),ProtocolLocation(......))
  val regex = """ConnectionDescriptor\(ProtocolLocation\((.*)\),ProtocolLocation\((.*)\)\)""".r

  def apply(s :String) = { 
    val regex(from,to) = s
    new ConnectionDescriptor(ProtocolLocation(from), ProtocolLocation(to))
  }
}

object NetworkingSystem { 
  // private var networks = new AtomicReference(collection.immutable.HashMap.empty[ProtocolLocationBase, AbstractNetwork])
  // def register(loc :ProtocolLocation, net :AbstractNetwork) { 
  //   val oldmap = networks.get
  //   if(! networks.compareAndSet(oldmap, oldmap.updated(loc, net))) { 
  //      println("retrying to register")
  //      register(loc, net)
  //   }
  // }
  // def isLocal(loc :ProtocolLocationBase) = { 
  //   networks.get.contains(loc)
  // }

  import ch.epfl.lsr.config._
  lazy val defaultOptions = Configuration.getMap("network")

  // ensure reading uses latest version
  @volatile
  private var systems  = collection.immutable.HashMap.empty[InetSocketAddress, NetworkingSystem]
  private val syslock = new Object()

  def apply(addr :InetSocketAddress, options :Map[String,Any] = null) = getSystem(addr, options)

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

  val dispatchingMap = new java.util.concurrent.ConcurrentHashMap[ProtocolLocation, AbstractNetwork]()

  def unbind(network :AbstractNetwork) = 
    dispatchingMap remove network.localId
  
  def bind(network :AbstractNetwork) :NetworkingSystem = { 
    if(network == null) { 
      throw new NullPointerException("network")
    }
    
    assert(network.localId.getSocketAddress == localAddress)

    dispatchingMap.put(network.localId, network)
    this
  }

  def getPipelineExtension(conn :ConnectionDescriptor) : Option[ChannelPipeline] = { 
    val network = dispatchingMap.get(conn.local) 

    if(network == null) { 
      None
    } else { 
      Some(network.newPipeline(conn))
    }
  }

  val serverBootstrap = { 
    val bs = SocketServer.bootstrap(options) { 
      pipeline (
	//new PrintWrittenHandler{ },
	// new PrintingHandler{ },
	new DispatchingHandler(getPipelineExtension _)
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
      remoteSelector
    )
  }

  private def copyPipeline(oldpipe :ChannelPipeline) :ChannelPipeline = { 
    val newpipe = pipeline()
    
    for(e <- oldpipe.toMap.entrySet) { 
      newpipe.addLast(e.getKey, e.getValue)
    }
    
    RemoteSelectionHandler.copyConnectionDescriptor(oldpipe, newpipe)
    newpipe
  }

  def makeClientPipeline(toAppend :ChannelPipeline) :ChannelPipeline = { 
    val future = clientBootstrap.bind(localAddress) 
    val pipeline = future.getChannel.getPipeline
    
    for(e <- toAppend.toMap.entrySet) { 
      pipeline.addLast(this.toString+"Extension"+e.getKey, e.getValue)
    }
    
    pipeline
  }
}


abstract class AbstractNetwork(val localId: ProtocolLocation) extends Network { 
  def name = localId.name

  private val system : NetworkingSystem = NetworkingSystem(localId.getSocketAddress).bind(this)

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
  // private def getSource(id :ProtocolLocation) = { 
  //   sources.synchronized{ 
  //     sources.get(id) 
  //   }
  // }

  private def getOrCreateSource(id :ProtocolLocation) :ChannelSource = { 
    sources.synchronized{ 
      sources.getOrElseUpdate(id, {
	val conn = localId to id
	val pipeline = system.makeClientPipeline(newPipeline(conn))
	
	RemoteSelectionHandler.setConnectionDescriptor(pipeline, conn)
	
	val source = pipeline.getLast.asInstanceOf[ChannelSource] 
	val future = source.connect()
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

  // creates a new Pipeline
  def newPipeline(conn :ConnectionDescriptor) = { 
    val source =  new ChannelSource(conn, onMessageReceived _) 
    val pipeline = ChannelPipelines.getPipeline(conn)
    pipeline.addLast("source", source)
    addSource(conn.remote, source)
    pipeline
  }
}


object ChannelPipelines { 
  private val map = new java.util.concurrent.ConcurrentHashMap[String,ChannelPipelineFactory]
  
  def register(scheme :String, factory :ChannelPipelineFactory) = { 
    map.put(scheme, factory)
  }

  register("lsr", new ChannelPipelineFactory { 
    def getPipeline = pipeline( 
      new KryoEncoder(),
      new KryoDecoder()
    )})
  
  def getPipeline(conn :ConnectionDescriptor) = { 
    val scheme = conn.local.scheme
    val factory = map.get(scheme)
    if(factory == null) 
      throw new Exception("no pipelinefactory set for scheme: "+scheme)
    else 
      factory.getPipeline
  }
}
