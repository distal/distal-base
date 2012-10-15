package ch.epfl.lsr.netty.channel

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.buffer.{ ChannelBuffer }
import org.jboss.netty.handler.codec.frame.{ DelimiterBasedFrameDecoder, Delimiters }

import ch.epfl.lsr.netty.util.ChannelFutures

import java.nio.charset.Charset

class DispatchingHandler(dispatch :String=>Option[ChannelPipeline]) extends DelimiterBasedFrameDecoder(1024, Delimiters.lineDelimiter() :_*) { 
  import scala.collection.JavaConversions._

  override def decode(ctx :ChannelHandlerContext, ch :Channel, cb :ChannelBuffer ) :Object = { 
    val frame = super.decode(ctx, ch, cb)

    if(frame == null)
      return null 
    
    val line = frame.asInstanceOf[ChannelBuffer].toString(Charset.defaultCharset)

    val optionallyAppend :Option[ChannelPipeline] = dispatch(line)


    if(optionallyAppend.isEmpty) {
      ch.close
      return null
    } else { 
      val pipeline = ctx.getPipeline()
      val toAppend = optionallyAppend.get
      
      for(e <- toAppend.toMap.entrySet) { 
	pipeline.addLast(this.toString+"Extension"+e.getKey, e.getValue)
      }
      
      pipeline remove this
    }

//    println("readable: "+cb.readableBytes)
    cb.readBytes(cb.readableBytes)
  }

  override def exceptionCaught(ctx :ChannelHandlerContext, e :ExceptionEvent) { 
    super.exceptionCaught(ctx, e)
  }
  
}


object RemoteSelectionHandler { 
  def setSelectionString(pipeline :ChannelPipeline, selection :String) = { 
    pipeline.getContext(classOf[RemoteSelectionHandler]).setAttachment(selection)
  }
  
  def getSelectionString(pipeline :ChannelPipeline) :String = { 
    pipeline.getContext(classOf[RemoteSelectionHandler]).getAttachment().asInstanceOf[String]
  }

  def copySelectionString(from :ChannelPipeline, to :ChannelPipeline) = { 
    setSelectionString(to, getSelectionString(from))
  }
}

class RemoteSelectionHandler extends SimpleChannelHandler  { 

  def getSelectionString(ctx :ChannelHandlerContext) = ctx.getAttachment.asInstanceOf[String]

  override def channelConnected(ctx :ChannelHandlerContext, e :ChannelStateEvent) { 
    super.channelConnected(ctx, e)
  }

  override def connectRequested(ctx :ChannelHandlerContext, e :ChannelStateEvent) { 
    ChannelFutures.onSuccess(e.getFuture) { 
      f => 
	val buffer =
	  // basicly copied from netty's StringEncoder
	  copiedBuffer(ctx.getChannel.getConfig.getBufferFactory.getDefaultOrder, getSelectionString(ctx)+"\n", Charset.defaultCharset)
	// send write event downstream
	Channels.write(ctx, Channels.succeededFuture(e.getChannel), buffer)
    }

    super.connectRequested(ctx, e)
  }
  
  override def exceptionCaught(ctx :ChannelHandlerContext, e :ExceptionEvent) { 
    ctx.sendUpstream(e)
  }
}
