package ch.epfl.lsr.netty.bootstrap

import org.jboss.netty.bootstrap.{ Bootstrap, ServerBootstrap, ClientBootstrap, ConnectionlessBootstrap }
import org.jboss.netty.channel._

import org.jboss.netty.channel.socket.nio.{ NioServerSocketChannelFactory, NioClientSocketChannelFactory, NioDatagramChannelFactory }
import org.jboss.netty.channel.socket.oio.{ OioServerSocketChannelFactory, OioClientSocketChannelFactory, OioDatagramChannelFactory }

import org.jboss.netty.channel.socket.{ DatagramChannelConfig, ServerSocketChannelConfig, SocketChannelConfig }
import org.jboss.netty.channel.socket.nio.{ NioSocketChannelConfig }

import com.typesafe.config.Config
import scala.reflect.ClassTag


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
  import com.typesafe.config._
  
  def configClass : Class[_]
  private lazy val configClassMethods = { 
    configClass.getDeclaredMethods
  }

  private def longOption(name :String, conf :Config) :Long = { 
    try { 
      conf.getInt(name).toLong
    } catch { 
      case e: ConfigException => 
	try { 
	  conf.getMilliseconds(name)
	} catch { 
	  case e: ConfigException => 
	    conf.getBytes(name).toLong
	}
    }
  }
  
  // TODO this does not really work. 
  def getArgumentType(key :String) = { 
    val name :String = "set"+(if(key.startsWith("child.")) key.substring("child.".length)
			     else key).toLowerCase
    println("checking "+name)
    val types = configClassMethods filter { 
      m => 
	val n = m.getName.toLowerCase
	println("  "+n+": "+(n==name)); 
	n == name
      } flatMap { _.getParameterTypes.headOption } 

    types.foreach { t => 
      println("  have "+t)
    }

    types.headOption.getOrElse(classOf[Null])
  }

  object types { 
    val Int = classOf[Int]
    val Long = classOf[Long]
    val String = classOf[String]
    val Boolean = classOf[Boolean]
    val NotFound = classOf[Null]
  }

  def setOptions(bootstrap :T with CanApplyPipeline[T], options :Config) : T with CanApplyPipeline[T] = { 
    println("---------------------------------------")
    println("setting options:")
    println (this.getClass+": "+configClass)


    options.entrySet.asScala.map(_.getKey).foreach { 
      key => 
	val value = getArgumentType(key) match { 
	  case types.Int => longOption(key, options).toInt
	  case types.Long => longOption(key, options)
	  //case types.String => options.getString(key)
	  case types.Boolean => options.getBoolean(key)
	  case types.NotFound => 
	    println("not found: "+key)
	    options.getValue(key).unwrapped
	  case _ => 
	    options.getValue(key).unwrapped
	}
      println("  "+key+" = "+value +" ("+value.getClass+")")
      bootstrap.setOption(key,value)
    }
    bootstrap
  }
}

object ChannelFactories { 
  import ch.epfl.lsr.util.execution.{ Executors => E }
  import scala.collection.JavaConverters._

  val IOBoss =   E.newCachedThreadPoolExecutor("IOBoss")
  val IOWorker = E.newCachedThreadPoolExecutor("IOWorker")


  private def useNIO(options :Config) :Boolean = { 
    try {  
      val key = options.root.keySet.asScala.filter { k => k.toLowerCase == "useNIO".toLowerCase }
      options.getBoolean(key.head)      
    } catch { 
      case e :Exception => 
	println("could not find useNIO option, turning on by default")
	true
    }
  }
  
  def server(options :Config) = { 
    useNIO(options) match { 
      case false => new OioServerSocketChannelFactory(IOBoss, IOWorker)
      case _ => new NioServerSocketChannelFactory(IOBoss, IOWorker)
    }
  }

  def client(options :Config) = { 
    useNIO(options) match { 
      case false => new OioClientSocketChannelFactory(IOWorker)
      case _ => new NioClientSocketChannelFactory(IOBoss, IOWorker)
    }
  }

  def datagram(options :Config) = { 
    useNIO(options) match { 
      case false => new OioDatagramChannelFactory(IOWorker)
      case _ => new NioDatagramChannelFactory(IOWorker)
    }
  }

}

object NIOSocketClient extends Bootstrapper[ClientBootstrap] { 
  def configClass = classOf[NioSocketChannelConfig] 
  def bootstrap(options :Config)  : ClientBootstrap with CanApplyPipeline[ClientBootstrap] = { 
    val bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory()) with CanApplyPipeline[ClientBootstrap] 
    setOptions(bootstrap, options)
  }
}

object NIOSocketServer extends Bootstrapper[ServerBootstrap] {
  val configClass = classOf[NioSocketChannelConfig]
  def bootstrap(options :Config)  : ServerBootstrap with CanApplyPipeline[ServerBootstrap]  = { 
    val bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory()) with CanApplyPipeline[ServerBootstrap]
    setOptions(bootstrap, options)
  }
}

object SocketServer extends Bootstrapper[ServerBootstrap] { 
  def configClass = classOf[SocketChannelConfig]
  def bootstrap(options :Config)  : ServerBootstrap with CanApplyPipeline[ServerBootstrap]  = { 
    val bootstrap = new ServerBootstrap(ChannelFactories.server(options)) with CanApplyPipeline[ServerBootstrap]
    setOptions(bootstrap, options)
  }
}

object SocketClient extends Bootstrapper[ClientBootstrap] { 
  def configClass = classOf[SocketChannelConfig]
  def bootstrap(options :Config)  : ClientBootstrap with CanApplyPipeline[ClientBootstrap]  = { 
    val bootstrap = new ClientBootstrap(ChannelFactories.client(options)) with CanApplyPipeline[ClientBootstrap]
    setOptions(bootstrap, options)
  }
}


object DatagramServer extends  Bootstrapper[ConnectionlessBootstrap] { 
  def configClass = classOf[DatagramChannelConfig]
  def bootstrap(options :Config)  : ConnectionlessBootstrap with CanApplyPipeline[ConnectionlessBootstrap]  = { 
    val bootstrap = new ConnectionlessBootstrap(ChannelFactories.datagram(options)) with CanApplyPipeline[ConnectionlessBootstrap]
    setOptions(bootstrap, options)
  }
}
