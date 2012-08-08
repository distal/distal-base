package ch.epfl.lsr.netty.network.akka

import ch.epfl.lsr.netty.network.{ NetworkingSystem, AbstractNetwork }
import ch.epfl.lsr.netty.channel.{ ChannelSource }
import ch.epfl.lsr.netty.protocol.{ ProtocolLocation }

import java.net.InetSocketAddress

import _root_.akka.actor._

class ActorNetworkingSystem(addr :InetSocketAddress, options :Map[String,Any]) extends NetworkingSystem(addr, options) { 

  def bind(actor :ActorRef, location :ProtocolLocation) :ActorNetwork = { 
    if(actor == null)
      throw new NullPointerException("actor")
    
    val network = new ActorNetwork(actor, location)
    assert(location.getSocketAddress == addr)
    network.bindTo(this)
    network
  }

}

class ActorNetwork(actor :ActorRef, location :ProtocolLocation) extends AbstractNetwork(location) { 
    
  def onMessageReceived(ctx :ChannelSource, msg :AnyRef) { 
    actor ! msg
  }
}
