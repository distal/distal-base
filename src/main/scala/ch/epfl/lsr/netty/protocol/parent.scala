package ch.epfl.lsr.netty.protocol

import ch.epfl.lsr.netty.network.{ AbstractNetwork, Network, NetworkingSystem }
import scala.collection.mutable.HashMap

abstract class ParentProtocol[T] extends Protocol { 
  case class Envelope(id :T, msg :Any)
  private val subs = HashMap.empty[T, SubProtocol[T]]

  def createSubProtocol(id :T) :Option[SubProtocol[T]]

  def onMessageReceived(m: Any, remoteLocation: ProtocolLocation) { 
    val env = m.asInstanceOf[Envelope]
    subs.get(env.id) match { 
      case Some(sub) => 
	sub.onMessageReceived(m, remoteLocation)
      case None => 
	createSubProtocol(env.id) match { 
	  case Some(sub) => 
	      subs.update(env.id, sub)
	      sub.onMessageReceived(m, remoteLocation)
	  case None => 
	    // ignore
	}
    }
  }
}

abstract class SubProtocol[T](val subId:T, parent :ParentProtocol[T]) extends Protocol { 
  override val network = new Network { 
    def sendTo(m: Any, ids: ProtocolLocation*) { 
      parent.network.sendTo(parent.Envelope(subId, m), ids :_*)
    }
    def forwardTo(m :Any, to :ProtocolLocation, from :ProtocolLocation) { 
      throw new UnsupportedOperationException("SubProtocol.Network.forwardTo")
    }
    def close = ()
  }  
}

