package ch.epfl.lsr.util.execution

import java.util.concurrent._


abstract class OrderedThreadPoolExecutor(corePoolSize :Int, maxPoolSize :Int, workerQueueSize :Int, keepAlive:Long, timeUnit :TimeUnit, name :String) extends Executor {

  lazy val underlyingPool = {
    val runnablesQueue = if(corePoolSize == maxPoolSize)
      new LinkedBlockingDeque[Runnable]
    else
      new SynchronousQueue[Runnable]
    val pool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive, timeUnit, runnablesQueue, ThreadFactories.newNamedThreadFactory(name))
    pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
      def rejectedExecution(r :Runnable, exec :ThreadPoolExecutor) {
        println("rejected q="+ pool.getQueue.size+" p="+pool.getPoolSize);
        throw new RuntimeException("RejectedExecution should not happen.")
        exec.getQueue.put(r)
      }
    })
    pool
  }

  def beforeExecute(t :Thread, r :Runnable) = {}
  def afterExecute(r :Runnable, t :Throwable) = {}

  val childExecutors = new ConcurrentHashMap[AnyRef, ChildExecutor]()

  def removeChildExecutorKey(key :AnyRef) {
    childExecutors.remove(key)
  }
  def getChildExecutorKey(task :Runnable) : AnyRef

  override def execute(task :Runnable) = {
    getChildExecutor(task).execute(task)
  }

  def executeNext(task :Runnable) = {
    getChildExecutor(task).executeNext(task)
  }

  def getChildExecutor(task:Runnable) :ChildExecutor = {
    import scala.collection.JavaConversions._

    val key = getChildExecutorKey(task)
    var executor = childExecutors.get(key)
    if(executor == null) {
      executor = new ChildExecutor(key)
      val oldExecutor = childExecutors.putIfAbsent(key, executor)
      if(oldExecutor != null) {
        executor = oldExecutor
      }
    }
    executor
  }


  class ChildExecutor(key :AnyRef) extends Runnable with Executor {
    import ChildExecutorState._
    private val state = new java.util.concurrent.atomic.AtomicInteger(IDLE)
    private val q = new LinkedBlockingDeque[Runnable](workerQueueSize)
    @volatile
    private var lastRun = now

    private def now :Long = System.currentTimeMillis

    def ensureScheduledInParentPool = {
      if(scheduleIfIdle)
        underlyingPool.execute(this)
    }


    def executeNext(task :Runnable) {
      q.putFirst(task)

      ensureScheduledInParentPool
    }

    def execute(task :Runnable) {
      q.put(task)

      ensureScheduledInParentPool
    }

    private def startIfScheduled = state.compareAndSet(SCHEDULED, RUNNING)
    private def scheduleIfIdle = state.compareAndSet(IDLE, SCHEDULED)
    private def setIdle = { assert(state.get == RUNNING); state.set(IDLE) }

    def run() {

      val ran = false

      if(startIfScheduled) {
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
         setIdle
         // reschedule for missed tasks
         if(q.peek !=null)
           ensureScheduledInParentPool
       }
      }
    }
    lastRun = now
  }


}

object ChildExecutorState  {
  final val IDLE = 0
  final val SCHEDULED = 1
  final val RUNNING = 2
}
