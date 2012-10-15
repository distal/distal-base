package ch.epfl.lsr.netty.util 

import org.jboss.netty.channel.{ ChannelFuture, ChannelFutureListener, Channel, Channels }

object ChannelFutures { 
  def onCompleted(future :ChannelFuture, handler : =>Unit) :ChannelFuture = { 
    onCompleted(future) { f => handler }
  }

  def onCompleted(future :ChannelFuture) (handler :ChannelFuture=>Unit) :ChannelFuture = { 
    future.addListener(new ChannelFutureListener() { 
      def operationComplete(f :ChannelFuture) { 
	assert(f==future)
	handler(f)
      }
    })
    future 
  }


  def onSuccess(future :ChannelFuture, handler : =>Unit) :ChannelFuture = { 
    onSuccess(future) { f => handler }
  }
  
  def onSuccess(future :ChannelFuture) (handler :ChannelFuture=>Unit) :ChannelFuture = { 
    future.addListener(new ChannelFutureListener() { 
      def operationComplete(f :ChannelFuture) { 
	assert(f==future)
	if(f.isSuccess)
	  handler(f)
      }
    })
    future
  }

  def onFailure(future :ChannelFuture, handler : =>Unit) :ChannelFuture = { 
    onSuccess(future) { f => handler }
  }
  
  def onFailure(future :ChannelFuture) (handler :ChannelFuture=>Unit) :ChannelFuture = { 
    future.addListener(new ChannelFutureListener() { 
      def operationComplete(f :ChannelFuture) { 
	assert(f==future)
	if(!f.isSuccess)
	  handler(f)
      }
    })
    future
  }


  def future(channel :Channel) = { 
    Channels.future(channel)
  }

  def apply(channel :Channel) = { 
    future(channel)
  }

}
