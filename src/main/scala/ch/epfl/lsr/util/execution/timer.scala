package ch.epfl.lsr.util.execution

import org.jboss.netty.util.{ HashedWheelTimer, TimerTask, Timeout }

import java.util.concurrent.TimeUnit

object Timer { 
  // default is 100 ... 
  private lazy val timer = new HashedWheelTimer(1, TimeUnit.MILLISECONDS)

  private def newTask(thunk : =>Unit) = { 
    new TimerTask() { def run(t :Timeout) = { thunk }}
  }

  def delay(amount :Int, unit :TimeUnit) (thunk : =>Unit) = { 
    timer.newTimeout(newTask(thunk), amount, unit)
  }

}

