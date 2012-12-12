package ch.epfl.lsr.util.execution

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor, SynchronousQueue, ArrayBlockingQueue }
import java.util.concurrent.{ Executors => JE }

object Executors {
  val defaultThreadCount = Runtime.getRuntime().availableProcessors()

  def newCachedThreadPoolExecutor(name :String) = JE.newCachedThreadPool(ThreadFactories.newNamedThreadFactory(name))
  // new ThreadPoolExecutor(2, defaultThreadCount, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), ThreadFactories.newNamedThreadFactory(name))

  def newCachedOrderedThreadPoolExecutor(keyFunction :Runnable=>Object, name :String) = { 
    new OrderedThreadPoolExecutor(0, Integer.MAX_VALUE, java.lang.Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue(10000), name) { 
       def getChildExecutorKey(task :Runnable) = keyFunction(task)
    }
  }

  def newFixedThreadPoolExecutor(nthreads :Int, keyFunction :Runnable=>Object, name :String) = { 
    new OrderedThreadPoolExecutor(nthreads, nthreads, java.lang.Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue(10000), name) { 
      def getChildExecutorKey(task :Runnable) = keyFunction(task)
    }
  }
}
