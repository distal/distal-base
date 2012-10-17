package ch.epfl.lsr.util.execution

import java.util.concurrent.ThreadFactory

object ThreadFactories { 
  def newNamedThreadFactory(nme :String) = { 
    new ThreadFactory { 
      val name = nme
      val group = new ThreadGroup(name + "-group")
      var count = 0

      def newThread(r :Runnable) = {
	count = count + 1
	new Thread(group, r, name +"-"+ count)
      }
    }
  }
}
