package ch.epfl.lsr.netty.channel

import ch.epfl.lsr.netty.network.{ ProtocolLocation, ConnectionDescriptor }

import ch.epfl.lsr.netty.execution.InDownPool

import org.jboss.netty.channel._
import ch.epfl.lsr.netty.util.{ ChannelFutures }
import java.net.{ SocketAddress, InetSocketAddress }



// the point to inject messages
class ChannelSource(val conn :ConnectionDescriptor, onMessageReceived :(Any,ProtocolLocation)=>Unit) extends SimpleChannelHandler with EmptyLifeCycleAwareChannelHandler {

  // context handling
  @volatile
  var theContext :ChannelHandlerContext = null

  override def afterAdd(ctx :ChannelHandlerContext) {
    theContext = ctx

    ctx.setAttachment(conn)

    val ch = ctx.getChannel
    if(ch!=null && ch.isConnected) {
      onConnectionEstablished(ch)
    }

    super.afterAdd(ctx)
  }


  override def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent) {
    onMessageReceived(e.getMessage, conn.remote)

    super.messageReceived(ctx, e)
  }

  // connection handling
  @volatile
  private var theChannel :Channel = null
  private var q = new scala.collection.mutable.Queue[Any]()

  def isConnected = theChannel != null && theChannel.isConnected

  def connect() = {
    val future = theContext.getChannel.connect(conn.remote.getSocketAddress)

    ChannelFutures.onCompleted(future) {
      f =>
	onConnectionEstablished(f.getChannel)
    }
    future
  }

  def close = {
    q.synchronized {
      val ch = theChannel
      theChannel = null
      Channels.close(ch)
    }
  }

  override def channelClosed(ctx :ChannelHandlerContext , e :ChannelStateEvent) {
    theChannel = null
    super.channelClosed(ctx, e)
  }

  def onConnectionEstablished(ch :Channel) {
    // println("Source("+conn+") connected")

    q.synchronized {
      theChannel = ch
      InDownPool.write(this, q.dequeueAll(_ => true) :_*)
    }
  }

  def write(msg :Any) {
    q.synchronized {
      if(isConnected)
	InDownPool.write(this, msg)
      else
	q.enqueue(msg)
    }
  }

  def getCurrentChannel = if (isConnected) Some(theChannel) else None

  // other stuff
  override def exceptionCaught(ctx :ChannelHandlerContext, e :ExceptionEvent) {
    e.getCause.printStackTrace
    e.getChannel.close
  }

}


object ChannelSource {

  def from(pipeline :ChannelPipeline) = {
    pipeline.getLast.asInstanceOf[ChannelSource]
  }

  def getConnectionDescriptor(pipeline :ChannelPipeline) :ConnectionDescriptor = {
    pipeline.getContext(classOf[ChannelSource]).getAttachment.asInstanceOf[ConnectionDescriptor]
  }

  def getConnectionDescriptor(channel :Channel) :ConnectionDescriptor = {
    getConnectionDescriptor(channel.getPipeline)
  }

}
