package ch.epfl.lsr.util.execution

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor, SynchronousQueue, ArrayBlockingQueue }
import java.util.concurrent.{ Executors => JE }

object Executors {
  val defaultThreadCount = 2*Runtime.getRuntime().availableProcessors()

  def newCachedThreadPoolExecutor(name :String) = JE.newCachedThreadPool(ThreadFactories.newNamedThreadFactory(name))
  // new ThreadPoolExecutor(2, defaultThreadCount, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), ThreadFactories.newNamedThreadFactory(name))

  def newCachedOrderedThreadPoolExecutor(keyFunction :Runnable=>Object, name :String, mailboxSize :Int) :OrderedThreadPoolExecutor = {
    new OrderedThreadPoolExecutor(defaultThreadCount, java.lang.Integer.MAX_VALUE, mailboxSize, 60L, TimeUnit.SECONDS, name) {
      def getChildExecutorKey(task :Runnable) = keyFunction(task)
    }
  }

  def newFixedThreadPoolExecutor(nthreads :Int, keyFunction :Runnable=>Object, name :String, mailboxSize :Int) = {
    new OrderedThreadPoolExecutor(nthreads, nthreads, mailboxSize, java.lang.Long.MAX_VALUE, TimeUnit.SECONDS, name) {
      def getChildExecutorKey(task :Runnable) = keyFunction(task)
    }
  }
}
