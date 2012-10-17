package ch.epfl.lsr.netty.execution

import org.jboss.netty.channel.{ Channel, Channels }
import ch.epfl.lsr.netty.channel.ChannelSource
import ch.epfl.lsr.netty.util.ChannelFutures
import java.nio.channels.ClosedChannelException

class DownStreamRunnable(val source :ChannelSource, msg :Any) extends Runnable { 
  def run() { 
    try { 
      source.getCurrentContext match { 
	case Some(ctx) => 
	  val ch = ctx.getChannel
	//println("DP write: "+msg+" "+source)
	  val future = Channels.write(ch, msg)
	  ChannelFutures.onCompleted(future) { 
	    future =>
	      assume(future.isDone)
	      val cause = future.getCause
	      if(cause!=null)
		println("Exception during write (in I/O thread): "+cause.getMessage+" while sending "+msg+" to "+ source.remoteLocation)
	  }
	case None => 
	  throw new Exception("failed to write "+msg+" to "+ source.remoteLocation +" (context unavailable)")
      }
    } catch { 
      case t :Throwable =>
	println("DownStreamRunnable caught "+t.getMessage+" while sending "+msg+" to "+ source.remoteLocation)
	t.printStackTrace
    }
  }
}


object InDownPool { 
  import ch.epfl.lsr.util.execution.Executors

  private val downPool =  Executors.newFixedThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), getSource _, "DownPool")  
    
  private def getSource(runnable :Runnable) = runnable.asInstanceOf[DownStreamRunnable].source
  
  def write(source :ChannelSource, msg :Any) { 
    downPool execute new DownStreamRunnable(source, msg)
  }
}

