package de.wittig.akka.router
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import akka.stream.ActorMaterializer
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object TestRouter extends App {

  implicit val system       = ActorSystem("Akka-Streams-Patterns")
  implicit val materializer = ActorMaterializer()
  implicit val timeout      = Timeout(10 seconds)
  import system.dispatcher

  val numberOfWorkers = 8

  val supervisor = system.actorOf(Props(new Supervisor(numberOfWorkers)))

  println("Hallo Welt")
  supervisor ! Start

  println("Hallo Weltende")

}

class Supervisor(val numberOfWorkers: Int) extends Actor {

  private val height    = 40
  private val length    = 60
  private var remaining = height * length

  val router: Router = {
    val routees = Vector.fill(numberOfWorkers) {
      val worker = context.actorOf(Props[Worker])
      context watch worker
      ActorRefRoutee(worker)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case Start => {
      for (y <- 0 until height; x <- 0 until length) {
        router.route(x + y, self)
      }
    }
    case Result(i) => {
      remaining = remaining - 1
      println(s"remaining: $remaining")
      if (remaining == 0) {
        println("finished")
        context.system.terminate()
      }
    }
  }
}

class Worker extends Actor {
  def receive = {
    case i: Int => {
      Thread.sleep(10)
      sender ! Result(i + 1)
    }
  }
}

case class Result(i: Int)
case object Start
