package ch.epfl.lsr.netty.protocol


trait Network { 
  def sendTo(m :Any, ids :ProtocolLocation*)
  def forwardTo(m :Any, to :ProtocolLocation, from :ProtocolLocation) 
  def close
  
  def onMessageReceived(msg :Any, from :ProtocolLocation)
}

object NetworkFactory { 
  import scala.collection.mutable.HashMap
  
  type Creator = Function2[ProtocolLocation, Protocol, Network]

  private val creators = new HashMap[String,Creator]()

  def newNetwork(loc :ProtocolLocation, protocol :Protocol) = { 
    val creator = creators(loc.scheme)

    creator(loc, protocol)
  }

  def registerScheme(scheme :String, creator :Creator) { 
    creators.update(scheme, creator)
  }
  
}
