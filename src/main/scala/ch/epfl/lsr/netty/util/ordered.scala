package ch.epfl.lsr.netty.util 

import org.jboss.netty.channel.{ Channels, ChannelHandlerContext, DownstreamMessageEvent }
import org.jboss.netty.handler.execution.{ OrderedDownstreamThreadPoolExecutor, ChannelDownstreamEventRunnable }


object InOrderedPool { 
  val downPool = new OrderedDownstreamThreadPoolExecutor(Runtime.getRuntime().availableProcessors())
  import Channels.{ future }

  def write(ctx :ChannelHandlerContext, msg :Any) { 
    val ch = ctx.getChannel
    val event = new DownstreamMessageEvent(ch,future(ch), msg, null)

    downPool.execute(new ChannelDownstreamEventRunnable(ctx, event, downPool))
  }
}

