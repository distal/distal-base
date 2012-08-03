package ch.epfl.lsr.netty.util 

import org.jboss.netty.channel.{ Channels, ChannelHandlerContext }

object InIOThread { 
  def write(ctx :ChannelHandlerContext, msg :Any) { 
    ctx.getPipeline.execute(new Runnable() { 
      def run() { 
	Channels.write(ctx, Channels.succeededFuture(ctx.getChannel()),msg);
      }
    })
  }
}

