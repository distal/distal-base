package ch.epfl.lsr.netty.execution

import ch.epfl.lsr.netty.protocol._
import java.lang.Runnable
  import ch.epfl.lsr.util.execution.Executors

object InProtocolPool { 
  private val pool = Executors.newCachedOrderedThreadPoolExecutor(extractProtocol _, "Protocols")
  
  private def extractProtocol(task :Runnable) :Protocol = { 
    if(task.isInstanceOf[ProtocolRunnable])
      task.asInstanceOf[ProtocolRunnable].protocol
    else
      throw new Exception("Not a ProtocolRunnable") 
  }

  def unregister(protocol :Protocol) = pool.removeChildExecutorKey(protocol)

  def execute(task :ProtocolRunnable) { 
    pool execute task
  }

  def execute(protocol :Protocol, task :Runnable) { 
    pool execute new DefaultProtocolRunnable(protocol, task) 
  }

  def execute(protocol :Protocol, task : =>Unit) { 
    pool execute new AbstractProtocolRunnable(protocol) { 
      def run = { task }
    }
  }

}
