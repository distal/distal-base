package ch.epfl.lsr.util.execution

import ch.epfl.lsr.protocol._
import java.lang.Runnable

object InProtocolPool {
  private val pool = Executors.newCachedOrderedThreadPoolExecutor(extractProtocol _, "Protocols", Integer.MAX_VALUE)

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

  def executeNext(task :ProtocolRunnable) {
    pool executeNext task
  }

  def executeNext(protocol :Protocol, task :Runnable) {
    pool executeNext new DefaultProtocolRunnable(protocol, task)
  }

  def executeNext(protocol :Protocol, task : =>Unit) {
    pool executeNext new AbstractProtocolRunnable(protocol) {
      def run = task
    }
  }
}
