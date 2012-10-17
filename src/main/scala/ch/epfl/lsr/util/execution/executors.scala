package ch.epfl.lsr.util.execution

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor, SynchronousQueue, ArrayBlockingQueue }
import java.util.concurrent.{ Executors => JE }

object Executors {
  val defaultThreadCount = Runtime.getRuntime().availableProcessors()

  def newCachedThreadPoolExecutor(name :String) = JE.newCachedThreadPool(ThreadFactories.newNamedThreadFactory(name))
  // new ThreadPoolExecutor(2, defaultThreadCount, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), ThreadFactories.newNamedThreadFactory(name))

  def newCachedOrderedThreadPoolExecutor(keyFunction :Runnable=>Object, name :String) = { 
    new OrderedThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue(), name) { 
       def getChildExecutorKey(task :Runnable) = keyFunction(task)
    }
  }

  def newFixedThreadPoolExecutor(nthreads :Int, keyFunction :Runnable=>Object, name :String) = { 
    new OrderedThreadPoolExecutor(nthreads, nthreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(), name) { 
      def getChildExecutorKey(task :Runnable) = keyFunction(task)
    }
  }
}
