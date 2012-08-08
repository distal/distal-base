package ch.epfl.lsr.netty.util 

import org.jboss.netty.channel.{ Channels, ChannelHandlerContext, DownstreamMessageEvent }
import org.jboss.netty.handler.execution.{ OrderedDownstreamThreadPoolExecutor, ChannelDownstreamEventRunnable }
import ch.epfl.lsr.netty.channel.ChannelSource

object InDownPool { 
  import ChannelFutures.implicits._

  val downPool = new OrderedDownstreamThreadPoolExecutor(Runtime.getRuntime().availableProcessors())

  private def write(ctx :ChannelHandlerContext, msg :Any, onFailure : Function1[Throwable,Unit]) { 
    val ch = ctx.getChannel
    val future = ChannelFutures.onFailure(ch) { 
      f => 
	onFailure(f.getCause)
    }
    ChannelFutures.onCompleted(ch) { f =>
      println("successfully sent("+msg+")")
    }

    val event = new DownstreamMessageEvent(ch, future, msg, null)

    downPool.execute(new ChannelDownstreamEventRunnable(ctx, event, downPool){  
      override def doRun() { 
	try { 
	  super.doRun
	} catch { 
	  case t :Throwable => 
	    println(t.getClass)
	    onFailure(t)
	  // throw t
	}
      }
    })
  }


  private def write(ctx :ChannelHandlerContext, msg :Any) { 
    write(ctx,msg, { e => () })
  }

  def write(source :ChannelSource, msg :Any) { 
    source.getCurrentContext match { 
      case Some(ctx) => write(ctx, msg); true
      case None => false
    }
  }
}

