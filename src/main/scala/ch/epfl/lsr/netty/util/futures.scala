package ch.epfl.lsr.netty.util 

import org.jboss.netty.channel.{ ChannelFuture, ChannelFutureListener }

object ChannelFutures { 

  def onCompleted(future :ChannelFuture, handler : =>Unit) { 
    onCompleted(future) { f => handler }
  }

  def onCompleted(future :ChannelFuture) (handler :ChannelFuture=>Unit) { 
    future.addListener(new ChannelFutureListener() { 
      def operationComplete(f :ChannelFuture) { 
	assert(f==future)
	handler(f)
      }
    })
  }


  def onSuccess(future :ChannelFuture, handler : =>Unit) { 
    onSuccess(future) { f => handler }
  }
  
  def onSuccess(future :ChannelFuture) (handler :ChannelFuture=>Unit) { 
    future.addListener(new ChannelFutureListener() { 
      def operationComplete(f :ChannelFuture) { 
	assert(f==future)
	if(f.isSuccess)
	  handler(f)
      }
    })
  }


}
