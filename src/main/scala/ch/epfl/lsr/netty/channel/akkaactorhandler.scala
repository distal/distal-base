package ch.epfl.lsr.netty.channel.akka

import ch.epfl.lsr.netty.channel.{ MessageReceivedHandler, EmptyLifeCycleAwareChannelHandler }

import org.jboss.netty.channel._

import akka.util.duration._
import akka.util.Timeout 
import akka.pattern.ask
import akka.actor._

import java.net.InetSocketAddress

import scala.collection.mutable.HashMap

object implicits { 
  implicit val timeout = Timeout(5 seconds)
}

class ActorForwardingHandler(actor :ActorRef) extends MessageReceivedHandler { 
  import implicits._

  override def messageReceived(ctx :ChannelHandlerContext, e :MessageEvent)  { 
    // uses implicit timeout
    val f = actor ? e.getMessage 
    f.onComplete { 
      case Right(answer) => 
        e.getChannel.write(answer)
      //case Left (e :AskTimeoutException) =>
      case _ => ()
    }
  }
}

