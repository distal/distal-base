package ch.epfl.lsr.netty.channel

import ch.epfl.lsr.netty.protocol.{ ProtocolLocation }

import ch.epfl.lsr.netty.execution.InDownPool

import org.jboss.netty.channel._
import ch.epfl.lsr.netty.util.{ ChannelFutures }
import java.net.{ SocketAddress, InetSocketAddress }

// the point to inject messages
class ChannelSource(localLocation :ProtocolLocation, registerRemoteAddress :(ProtocolLocation,ChannelSource)=>Unit, onMessageReceived :(Any,ProtocolLocation)=>Unit) extends SimpleChannelHandler with EmptyLifeCycleAwareChannelHandler { 
  
  // context handling
  @volatile 
  var theContext :ChannelHandlerContext = null
  
  override def afterAdd(ctx :ChannelHandlerContext) { 
    theContext = ctx 
    super.afterAdd(ctx)

    if(ctx.getChannel!=null && theContext.getChannel.isConnected) { 
	onConnectionEstablished
    }      
  }

  def getCurrentContext = { 
    if((theContext == null) || (theContext.getChannel == null))
      None 
    else
      Some(theContext)
  }

  // remote location handling  
  @volatile
  private var theRemoteLocation :ProtocolLocation = null
  lazy val remoteLocation = { 
    if(theRemoteLocation == null) 
      throw new Exception("theRemote == null")
    else 
      theRemoteLocation
  }
  
  private def setRemoteLocation(remote :ProtocolLocation) { 
    theRemoteLocation = remote
    registerRemoteAddress(remoteLocation, this)
  }

  override def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent) { 
    e.getMessage match { 
      case loc :ProtocolLocation => 
	setRemoteLocation(loc)
      case m => 
	onMessageReceived(m, this.remoteLocation)
    }
    
    super.messageReceived(ctx, e)
  }

  def close = { 
    Channels.close(theContext.getChannel)
  }

  // connection handling
  @volatile
  var connected = false
  @volatile
  var q = new scala.collection.mutable.SynchronizedQueue[Any]()

  def connect(other :ProtocolLocation) = { 
    setRemoteLocation(other)
    write(localLocation)  /* tell other side who we are */
    val future = theContext.getChannel.connect(other.getSocketAddress)

    ChannelFutures.onCompleted(future) { 
      f =>
	onConnectionEstablished()
    }
    future
  }

  override def channelClosed(ctx :ChannelHandlerContext , e :ChannelStateEvent) { 
    connected = false
  }
  
  def onConnectionEstablished() { 
    if(connected == false) { 
      // we write first to keep fifo
      while(q.nonEmpty) { 
	InDownPool.write(this, q.dequeue)
      }
      // someone might write/enqueue here, so ...
      connected = true
      // ... empty q again. 
      while(q.nonEmpty) { 
	  InDownPool.write(this, q.dequeue)
      }
    }
  }

  // other stuff
  override def exceptionCaught(ctx :ChannelHandlerContext, e :ExceptionEvent) { 
    e.getCause.printStackTrace
    e.getChannel.close
  }

  def write(msg :Any) { 
    if(connected)
      InDownPool.write(this, msg) 
    else
      q.enqueue(msg)
  }
}


object Source { 

  def from(pipeline :ChannelPipeline) = { 
    pipeline.getLast.asInstanceOf[ChannelSource]
  }

}
