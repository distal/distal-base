package ch.epfl.lsr.protocol


trait Network { 
  def sendTo(m :Any, ids :ProtocolLocation*)
  def close = NetworkFactory.close(this)

  def onMessageReceived(msg :Any, from :ProtocolLocation)
}

trait NetworkFactory { 
  def createNetwork(location :ProtocolLocation, protocol :Protocol) :Network 
}

// TODO: allow simulation to change local delivery
object NetworkFactory { 
  import scala.collection.immutable.{ HashMap => IHashMap }
  import scala.collection.mutable.{ HashMap => MHashMap }
  
  private val creators = new MHashMap[String,NetworkFactory]()

  private val lock = new Object
  @volatile
  private var locals = IHashMap.empty[ProtocolLocation, Network]

  def getLocal(loc :ProtocolLocation) :Option[Network]= locals.get(loc)

  def createNetwork(loc :ProtocolLocation, protocol :Protocol) = { 
    val creator = creators.synchronized { 
      creators(loc.scheme) 
    }
    
    val network = creator.createNetwork(loc, protocol)

    lock.synchronized{ locals = locals.updated(loc, network) }
    
    network
  }
  
  def close(network :Network) { 
    lock.synchronized{ 
      locals = locals.filter{ kv => kv._2 == network }
    }
  }

  def registerScheme(scheme :String, creator :NetworkFactory) { 
    creators.update(scheme, creator)
  }

  def registerScheme(scheme :String, creatorClazz :Class[_ <: NetworkFactory]) { 
    creators.find (kv => kv._2.getClass == creatorClazz ) match { 
      case Some(kv) => registerScheme(scheme, kv._2)
      case None => registerScheme(scheme, creatorClazz.newInstance)
    }
  }

  def registerFromConfig() = { 
    NetworkConfig.configuredSchemas.foreach { 
      s => 
	val name = NetworkConfig(s).getString("factory")
	val clazz = Class.forName(name).asSubclass(classOf[NetworkFactory])
	registerScheme(s, clazz)
    }
  }

  registerFromConfig()
}
