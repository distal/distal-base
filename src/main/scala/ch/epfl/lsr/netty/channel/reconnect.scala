package ch.epfl.lsr.netty.channel

import org.jboss.netty.channel._
import org.jboss.netty.bootstrap.ClientBootstrap

import java.net.SocketAddress
import java.util.concurrent.TimeUnit

import ch.epfl.lsr.netty.util.Timer._

object ReconnectionHandler { 
  private def getState(ctx :ChannelHandlerContext) = { 
    var rv = ctx.getAttachment.asInstanceOf[ReconnectionState]
    if(rv == null) { 
      rv = new ReconnectionState
      ctx setAttachment rv
    }
    rv
  }

  class ReconnectionState() { 
    @volatile
    var reconnect = false
    @volatile
    var remoteAddress :SocketAddress = _
    @volatile
    var channelFactory :ChannelFactory = _
  }
}

// reconnects after connection is closed, copies attachments.
class ReconnectionHandler(reconnectionTimoutMillis :Int, copyPipeline: ChannelPipeline=>ChannelPipeline) extends SimpleChannelHandler  { 
  import ReconnectionHandler._

  override def closeRequested(ctx :ChannelHandlerContext , e :ChannelStateEvent) { 
    getState(ctx).reconnect = false

    println("close requested")
    
    super.closeRequested(ctx, e)
  }

  override def disconnectRequested(ctx :ChannelHandlerContext , e :ChannelStateEvent) { 
    getState(ctx).reconnect = false
    
    super.disconnectRequested(ctx, e)
  }

  override def connectRequested(ctx :ChannelHandlerContext , e :ChannelStateEvent) { 
    val state = getState(ctx)
    state.remoteAddress = e.getValue.asInstanceOf[SocketAddress]
    state.channelFactory = e.getChannel.getFactory
    state.reconnect = true    
    
    // println("connect requested in reconnector "+e.getChannel)

    super.connectRequested(ctx, e)
  }


  override def channelClosed(ctx :ChannelHandlerContext , e :ChannelStateEvent) { 
    delay(reconnectionTimoutMillis, TimeUnit.MILLISECONDS) { 
      if(getState(ctx).reconnect) { 
	// reuse the current pipeline and reconnect
 	println("reconnecting to "+getState(ctx).remoteAddress +" "+new java.util.Date)
	getState(ctx).channelFactory.newChannel(copyPipeline(ctx.getPipeline)).connect(getState(ctx).remoteAddress)
      }
    }
    super.channelClosed(ctx, e)
  }
}
