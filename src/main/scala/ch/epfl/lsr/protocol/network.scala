package ch.epfl.lsr.protocol


trait Network { 
  def sendTo(m :Any, ids :ProtocolLocation*)
  def close = NetworkFactory.close(this)

  def onMessageReceived(msg :Any, from :ProtocolLocation)
}

// TODO: allow simulation to change local delivery
object NetworkFactory { 
  import scala.collection.immutable.{ HashMap => IHashMap }
  import scala.collection.mutable.{ HashMap => MHashMap }
  
  type Creator = Function2[ProtocolLocation, Protocol, Network]
  private val creators = new MHashMap[String,Creator]()

  private val lock = new Object
  @volatile
  private var locals = IHashMap.empty[ProtocolLocation, Network]

  def getLocal(loc :ProtocolLocation) :Option[Network]= locals.get(loc)

  def newNetwork(loc :ProtocolLocation, protocol :Protocol) = { 
    val creator = creators.synchronized { 
      if(creators.isEmpty)
	Protocol.registerDefault
      creators(loc.scheme) 
    }
    
    val network = creator(loc, protocol)

    lock.synchronized{ locals = locals.updated(loc, network) }
    
    network
  }
  
  def close(network :Network) { 
    lock.synchronized{ 
      locals = locals.filter{ kv => kv._2 == network }
    }
  }

  def registerScheme(scheme :String, creator :Creator) { 
    creators.update(scheme, creator)
  }
}
