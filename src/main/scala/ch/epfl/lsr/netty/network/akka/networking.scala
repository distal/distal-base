package ch.epfl.lsr.netty.network.akka

import ch.epfl.lsr.netty.network.{ NetworkingSystem, ProtocolNetwork }
import ch.epfl.lsr.netty.channel.{ ChannelSource }

import java.net.InetSocketAddress

import _root_.akka.actor._

class ActorNetworkingSystem(addr :InetSocketAddress, options :Map[String,Any]) extends NetworkingSystem(addr.getHostName, addr.getPort, options) { 

  def bind(actor :ActorRef, name: String) :ActorNetwork = { 
    if(actor == null)
      throw new NullPointerException("actor")
    
    val network = new ActorNetwork(actor, name, this)
    bind(network, name)
    network
  }

}

class ActorNetwork(actor :ActorRef, name: String, system :NetworkingSystem) extends ProtocolNetwork(name, system) { 
  def onMessageReceived(ctx :ChannelSource, msg :AnyRef) { 
    actor ! msg
  }
}
