package ch.epfl.lsr.netty.channel

import org.jboss.netty.channel.SimpleChannelHandler
import org.jboss.netty.channel.{ ChannelHandlerContext, MessageEvent, ExceptionEvent, LifeCycleAwareChannelHandler }



trait MessageReceivedHandler extends SimpleChannelHandler  { 

  def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent) 

  override def exceptionCaught(ctx :ChannelHandlerContext, e :ExceptionEvent) { 
    e.getCause.printStackTrace
    e.getChannel.close
  }
}

trait EmptyLifeCycleAwareChannelHandler extends LifeCycleAwareChannelHandler { 
  def afterAdd(ctx :ChannelHandlerContext) {}
  def afterRemove(ctx :ChannelHandlerContext) {}

  def beforeAdd(ctx :ChannelHandlerContext) {}
  def beforeRemove(ctx :ChannelHandlerContext) {}
}


trait PrintingHandler extends MessageReceivedHandler { 
  override def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent) = { 
    println(e)

    ctx.sendUpstream(e)
  }

}
