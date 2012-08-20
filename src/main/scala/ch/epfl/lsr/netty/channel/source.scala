package ch.epfl.lsr.netty.channel

import ch.epfl.lsr.netty.protocol.{ ProtocolLocation }

import org.jboss.netty.channel._

// the point to inject messages (using InDownPool)
trait ChannelSource extends EmptyLifeCycleAwareChannelHandler { 
  @volatile 
  var theContext :ChannelHandlerContext = null
  def remoteLocation :ProtocolLocation 

  override def beforeAdd(ctx :ChannelHandlerContext) { 
    theContext = ctx 
  }
  
  def getCurrentContext = { 
    if(theContext != null && theContext.getChannel.isOpen)
      Some(theContext)
    else 
      None
  }

  def close = { 
    theContext.getChannel.close
  }
}
