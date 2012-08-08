package ch.epfl.lsr.netty.execution

import ch.epfl.lsr.netty.protocol._
import java.lang.Runnable

object InProtocolPool { 
  private val pool = OrderedThreadPoolExecutor.newCachedOrderedThreadPool(extractProtocol _)
  
  private def extractProtocol(task :Runnable) :Protocol = { 
    if(task.isInstanceOf[ProtocolRunnable])
      task.asInstanceOf[ProtocolRunnable].protocol
    else
      throw new Exception("Not a ProtocolRunnable") 
  }
  

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
