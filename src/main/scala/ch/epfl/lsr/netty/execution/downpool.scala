package ch.epfl.lsr.netty.execution

import org.jboss.netty.channel.{ Channel, Channels }
import ch.epfl.lsr.netty.channel.ChannelSource
import ch.epfl.lsr.netty.util.ChannelFutures
import java.nio.channels.ClosedChannelException

class DownStreamRunnable(val source :ChannelSource, msgs :Seq[Any]) extends Runnable {
  def writeMessage(ch :Channel, msg :Any) {
    val future = Channels.write(ch, msg)
    ChannelFutures.onCompleted(future) {
      future =>
      assume(future.isDone)
      val cause = future.getCause
      if(cause!=null)
        println("Exception during write (in I/O thread): "+cause.getMessage+" while sending "+msg+" to "+ source.conn.remote)
    }
  }

  def run() {
    try {
      source.getCurrentChannel match {
        case Some(ch) =>
          //println("DP write: "+msg+" "+source)
          msgs.foreach(m => writeMessage(ch, m))
        case None =>
          throw new Exception("failed to write "+msgs+" to "+ source.conn.remote +" (context unavailable)")
      }
    } catch {
      case t :Throwable =>
        println("DownStreamRunnable caught "+t.getMessage+" while sending "+msgs+" to "+ source.conn.remote)
        t.printStackTrace
    }
  }
}


object InDownPool {
  import ch.epfl.lsr.util.execution.Executors

  private val downPool =  Executors.newFixedThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), getSource _, "DownPool")

  private def getSource(runnable :Runnable) = runnable.asInstanceOf[DownStreamRunnable].source

  def write(source :ChannelSource, msg :Any*) {
    downPool execute new DownStreamRunnable(source, msg)
  }
}
