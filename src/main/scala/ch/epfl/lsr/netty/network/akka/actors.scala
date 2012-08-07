package ch.epfl.lsr.netty.network.akka

import com.typesafe.config._
import ch.epfl.lsr.netty.config._

import akka.actor._

import java.net.{ SocketAddress, InetSocketAddress }

object implicitConversions { 
  implicit def ActorLocation2SocketAddress(id :ActorLocation) :SocketAddress = id.getSocketAddress
  implicit def Config2ActorConfig(config :Config) : ActorConfig = new ActorConfig(config)
}

object ActorConfig { 
  private val validActorSystemNameChars = ('a' to 'z') ++ ('A' to 'Z') ++ Seq('_') ++ ('0' to '9')
  private def mangleSystemName(s :String) = s.map{c => validActorSystemNameChars.find(_==c).getOrElse('_') }
}

class ActorConfig(c :Config) { 
  def createActor = { 
    val actorClazz = c.getClazz[ActorWithNetworkAndConfig]("class")
    //implicit val manifest = ClassManifest.fromClass[Actor](actorClazz)
    val system = ActorSystem(ActorConfig.mangleSystemName(c.getURI("location").toString))
    system.actorOf(Props(actorClazz.getDeclaredConstructor(Seq(classOf[Config]) :_*).newInstance(Seq(c) :_*)))    
  }
}

case class ActorLocation(name :String, host :String, port :Int) { 
  def this(u :java.net.URI) = this(u.getPath, u.getHost, u.getPort)
  lazy val getSocketAddress = new InetSocketAddress(host, port)
}

object ActorWithNetwork { 
  lazy val networkOptions = Configuration.getMap("network")

  def createNetworkingSystem(location :ActorLocation) = { 
    println("creating network "+location.name)
    new NetworkingSystem(location.getSocketAddress, networkOptions)
  }
}

trait ActorWithNetworkAndConfig extends Actor { 
  def network : ActorNetwork
  def config : Config
}

abstract class ActorFromConfig(val config :Config) extends ActorWithNetworkAndConfig { 
  val network = { 
    val location = new ActorLocation(config.getURI("location"))
    val networkingSystem = ActorWithNetwork.createNetworkingSystem(location)
    networkingSystem.bind(self, location.name)
  }
}
