package ch.epfl.lsr.netty.channel

//import org.jboss.netty.channel.{ SimpleChannelHandler, SimpleUpstreamHandler }
//import org.jboss.netty.channel.{ ChannelHandlerContext, ChannelStateEvent, MessageEvent, ExceptionEvent, LifeCycleAwareChannelHandler }

import org.jboss.netty.channel._

// the point to inject messages (using InDownPool)
trait ChannelSource extends EmptyLifeCycleAwareChannelHandler { 
  @volatile 
  var theContext :ChannelHandlerContext = null

  override def beforeAdd(ctx :ChannelHandlerContext) { 
    theContext = ctx 
  }
  
  def getCurrentContext = { 
    if(theContext != null && theContext.getChannel.isOpen)
      Some(theContext)
    else 
      None
  }
}
