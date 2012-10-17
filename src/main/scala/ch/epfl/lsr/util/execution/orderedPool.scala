package ch.epfl.lsr.util.execution

import java.util.concurrent._


abstract class OrderedThreadPoolExecutor(corePoolSize :Int, maxPoolSize :Int, keepAlive:Long, timeUnit :TimeUnit, workQueue :BlockingQueue[Runnable], name :String) extends ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive, timeUnit, workQueue, ThreadFactories.newNamedThreadFactory(name)) { 
  
  val childExecutors = new ConcurrentHashMap[AnyRef, ChildExecutor]()
  
  def removeChildExecutorKey(key :AnyRef) { 
    childExecutors.remove(key)
  }
  def getChildExecutorKey(task :Runnable) : AnyRef

  override def execute(task :Runnable) = { 
    getChildExecutor(task).execute(task)    
  }

  def executeInParentPool(task :ChildExecutor) { 
    super.execute(task)
  }

  def getChildExecutor(task:Runnable) :Executor = { 
    val key = getChildExecutorKey(task)
    var executor = childExecutors.get(key)
    if(executor == null) { 
      executor = new ChildExecutor
      val oldExecutor = childExecutors.putIfAbsent(key, executor)
      if(oldExecutor != null) { 
	executor = oldExecutor
      }
    }
    return executor
  }

  class ChildExecutor() extends Runnable with Executor { 
    private val isRunning = new java.util.concurrent.atomic.AtomicBoolean(false)
    private val q = new LinkedBlockingQueue[Runnable]()
    @volatile 
    private var lastRun = now

    private def now :Long = System.currentTimeMillis
    

    def execute(task :Runnable) { 
      q.offer(task) 

      if(!isRunning.get) { 
	executeInParentPool(this)
      }
    }

    private def alreadyRunning = isRunning.compareAndSet(false, true)

    def run() { 
      val ran = false

      if(alreadyRunning) { 
	try { 
	  val currentThread = Thread.currentThread
	  var thrown :Throwable = null
	  var task :Runnable = null
	  
	  while({ task = q.poll; task != null}) { 
	    
	    beforeExecute(currentThread, task)
	    try { 
	      task.run
	    } catch { 
	      case t :Throwable => 
		println("_____"+t)
		thrown = t; throw t
	    } finally { 
	      afterExecute(task, thrown)
	    }
	  }
	} finally { 
	  isRunning.set(false)
	  // reschedule for missed tasks
	  if(q.peek !=null && !isRunning.get)
	    executeInParentPool(this)
	}
      }
    }
    lastRun = now
  }


}
