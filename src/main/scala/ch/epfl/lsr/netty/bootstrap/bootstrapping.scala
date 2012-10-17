package ch.epfl.lsr.netty.bootstrap

import org.jboss.netty.bootstrap.{ Bootstrap, ServerBootstrap, ClientBootstrap, ConnectionlessBootstrap }
import org.jboss.netty.channel._

import org.jboss.netty.channel.socket.nio.{ NioServerSocketChannelFactory, NioClientSocketChannelFactory, NioDatagramChannelFactory }
import org.jboss.netty.channel.socket.oio.{ OioServerSocketChannelFactory, OioClientSocketChannelFactory, OioDatagramChannelFactory }

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
  import scala.collection.JavaConverters._
  
  def bootstrap(options :Map[String,Any]) : T with CanApplyPipeline[T] 

  def bootstrap(options :Tuple2[String,Any]*) : T with CanApplyPipeline[T] = { 
    bootstrap(options.toMap)
  }

  def setOptions(bootstrap :T with CanApplyPipeline[T], options:Map[String,Any]) : T with CanApplyPipeline[T] = { 
    setOptions(bootstrap, options.view)
  }

  def setOptions(bootstrap :T with CanApplyPipeline[T], options:Traversable[(String,Any)]) : T with CanApplyPipeline[T] = { 
    println("setting options:")
    options.foreach(o => println("  "+o))
    options.foreach((bootstrap.setOption _).tupled)
    bootstrap
  }
}

object ChannelFactories { 
  import ch.epfl.lsr.util.execution.{ Executors => E }

  val IOBoss =   E.newCachedThreadPoolExecutor("IOBoss")
  val IOWorker = E.newCachedThreadPoolExecutor("IOWorker")


  private def useNIO(options :Map[String,Any]) :Boolean = { 
    val rv = 
    if(options==null)
      true
    else { 
      options.find(sa => sa._1 == "UseNIO") match { 
	case Some((_, false)) => false
	case _ => true
      }
    } 
    rv
  }
  
  def server(options :Map[String,Any]) = { 
    useNIO(options) match { 
      case false => new OioServerSocketChannelFactory(IOBoss, IOWorker)
      case _ => new NioServerSocketChannelFactory(IOBoss, IOWorker)
    }
  }

  def client(options :Map[String,Any]) = { 
    useNIO(options) match { 
      case false => new OioClientSocketChannelFactory(IOWorker)
      case _ => new NioClientSocketChannelFactory(IOBoss, IOWorker)
    }
  }

  def datagram(options :Map[String,Any]) = { 
    useNIO(options) match { 
      case false => new OioDatagramChannelFactory(IOWorker)
      case _ => new NioDatagramChannelFactory(IOWorker)
    }
  }

}

object NIOSocketClient extends Bootstrapper[ClientBootstrap] { 
  def bootstrap(options :Map[String,Any])  : ClientBootstrap with CanApplyPipeline[ClientBootstrap] = { 
    val bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory()) with CanApplyPipeline[ClientBootstrap] 
    setOptions(bootstrap, options)
  }
}

object NIOSocketServer extends Bootstrapper[ServerBootstrap] { 
  def bootstrap(options :Map[String,Any])  : ServerBootstrap with CanApplyPipeline[ServerBootstrap]  = { 
    val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory()) with CanApplyPipeline[ServerBootstrap]
    setOptions(bootstrap, options)
  }
}

object SocketServer extends Bootstrapper[ServerBootstrap] { 
  def bootstrap(options :Map[String,Any])  : ServerBootstrap with CanApplyPipeline[ServerBootstrap]  = { 
    val bootstrap = new ServerBootstrap(ChannelFactories.server(options)) with CanApplyPipeline[ServerBootstrap]
    setOptions(bootstrap, options)
  }
}

object SocketClient extends Bootstrapper[ClientBootstrap] { 
  def bootstrap(options :Map[String,Any])  : ClientBootstrap with CanApplyPipeline[ClientBootstrap]  = { 
    val bootstrap = new ClientBootstrap(ChannelFactories.client(options)) with CanApplyPipeline[ClientBootstrap]
    setOptions(bootstrap, options)
  }
}


object DatagramServer extends  Bootstrapper[ConnectionlessBootstrap] { 
  def bootstrap(options :Map[String,Any])  : ConnectionlessBootstrap with CanApplyPipeline[ConnectionlessBootstrap]  = { 
    val bootstrap = new ConnectionlessBootstrap(ChannelFactories.datagram(options)) with CanApplyPipeline[ConnectionlessBootstrap]
    setOptions(bootstrap, options)
  }
}
