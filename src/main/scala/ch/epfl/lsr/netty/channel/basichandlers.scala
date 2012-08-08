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


trait PrintingHandler extends SimpleChannelHandler { 
  val charset = java.nio.charset.Charset.defaultCharset()

  def print(prefix :String, e :Any, postfix :String) { 
    if(e.isInstanceOf[MessageEvent]) { 
      print(prefix, e.asInstanceOf[MessageEvent].getMessage, postfix)
    } else if(e.isInstanceOf[ChannelBuffer]) { 
      println(prefix+e.asInstanceOf[ChannelBuffer].toString(charset)+postfix)
    } else { 
      println(prefix+e.toString+postfix)
    }
  }

  def timePostfix = " "+java.text.DateFormat.getTimeInstance.format(new java.util.Date)

  override def handleUpstream(ctx :ChannelHandlerContext, e :ChannelEvent) = { 
    print(" *U* ", e, timePostfix)
    println("____ "+e.getChannel.isOpen)
    //ctx.sendUpstream(e)
    super.handleUpstream(ctx, e)
  }

  override def handleDownstream(ctx :ChannelHandlerContext, e :ChannelEvent) = { 
    print(" +D+ ", e, timePostfix)
    println("____ "+e.getChannel.isOpen)
    super.handleDownstream(ctx, e)
  }

  override def exceptionCaught(ctx :ChannelHandlerContext, e :ExceptionEvent) { 
    // e.getCause.printStackTrace
    print(" ^E^ ", e.getCause, timePostfix)
    println("____ "+e.getChannel.isOpen)
    e.getCause.printStackTrace
  }

}
}
