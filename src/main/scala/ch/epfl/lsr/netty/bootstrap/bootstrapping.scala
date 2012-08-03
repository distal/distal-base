package ch.epfl.lsr.netty.bootstrap

import org.jboss.netty.bootstrap.{ Bootstrap, ServerBootstrap, ClientBootstrap }
import org.jboss.netty.channel._

import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

object PipelineHelper { 
  
  def pipelineFactory(f : =>ChannelPipeline) = { 
    new ChannelPipelineFactory { 
      def getPipeline = { f }
    }
  }
}

object pipeline { 

  def apply(handler :ChannelHandler*) :ChannelPipeline = { 
    Channels.pipeline(handler :_*) 
  }

}

trait CanApplyPipeline[T] { 
  self :T =>

  def setPipelineFactory(pipelineFactory : ChannelPipelineFactory)
    
  def apply(pipelineFactory : ChannelPipelineFactory) :T = { 
    setPipelineFactory(pipelineFactory)
    self
  }

  def apply (pipeline : =>ChannelPipeline) :T = { 
    
    setPipelineFactory(PipelineHelper.pipelineFactory{ pipeline })
    self
  }
}


trait Bootstrapper[T <: Bootstrap] { 
  def bootstrap(options :Tuple2[String,Any]*) : T with CanApplyPipeline[T] 

  def setOptions(bootstrap :T with CanApplyPipeline[T], options:Seq[(String,Any)]) : T with CanApplyPipeline[T] = { 
    options.foreach((bootstrap.setOption _).tupled)
    bootstrap
  }
}

object NIOSocketCient extends Bootstrapper[ClientBootstrap] { 
  def bootstrap(options :Tuple2[String,Any]*)  : ClientBootstrap with CanApplyPipeline[ClientBootstrap] = { 
    val bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory()) with CanApplyPipeline[ClientBootstrap] 
    setOptions(bootstrap, options)
  }
}

object NIOSocketServer extends Bootstrapper[ServerBootstrap] { 
  def bootstrap(options :Tuple2[String,Any]*)  : ServerBootstrap with CanApplyPipeline[ServerBootstrap]  = { 
    val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory()) with CanApplyPipeline[ServerBootstrap]
    setOptions(bootstrap, options)
  }
}

