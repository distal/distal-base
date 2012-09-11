package ch.epfl.lsr.netty.execution

import org.jboss.netty.channel.{ Channels, ChannelHandlerContext, DownstreamMessageEvent }
import org.jboss.netty.handler.execution.{ OrderedDownstreamThreadPoolExecutor, ChannelDownstreamEventRunnable }
import ch.epfl.lsr.netty.channel.ChannelSource
import ch.epfl.lsr.netty.util.{ ChannelFutures }
import java.nio.channels.ClosedChannelException

object InDownPool { 
  import ChannelFutures.implicits._

  val downPool = new OrderedThreadPoolExecutor.FixedThreadPool(2*Runtime.getRuntime().availableProcessors()) { 
    def getChildExecutorKey(task :Runnable) = { 
      val ch = task.asInstanceOf[ChannelDownstreamEventRunnable].getEvent.getChannel
      if(!ch.isOpen)
	throw new ClosedChannelException
      ch
    }
  }
  
  private def write(ctx :ChannelHandlerContext, msg :Any, onFailure : Function1[Throwable,Unit]) { 
    val ch = ctx.getChannel
    val future = ChannelFutures.onFailure(ch) { 
      f => 
	onFailure(f.getCause)
    }
    // ChannelFutures.onCompleted(ch) { f =>
    //   println("successfully sent("+msg+")")
    // }

    val event = new DownstreamMessageEvent(ch, future, msg, null)

    downPool.execute(new ChannelDownstreamEventRunnable(ctx, event, downPool){  
      override def doRun() { 
	try { 
	  super.doRun
	  // println("ran for "+msg)
	} catch { 
	  case t :Throwable => 
	    println("caught "+t+" while running for "+msg)
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
      case Some(ctx) => write(ctx, msg)
      case None => println("failed to write "+msg+" (context unavailable)")
    }
  }
}

