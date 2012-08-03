package ch.epfl.lsr.netty.util 

import org.jboss.netty.util.{ HashedWheelTimer, TimerTask, Timeout }

import java.util.concurrent.TimeUnit

object Timer { 
 
  private lazy val timer = new HashedWheelTimer()

  private def newTask(thunk : =>Unit) = { 
    new TimerTask() { def run(t :Timeout) = { thunk }}
  }

  def delay(amount :Int, unit :TimeUnit) (thunk : =>Unit) = { 
    timer.newTimeout(newTask(thunk), amount, unit)
  }

}

