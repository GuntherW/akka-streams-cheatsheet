package de.wittig.akka
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

package object streams {

  def time[A](block: => Future[A]): Future[A] = {
    val t0                = System.nanoTime()
    val result: Future[A] = block
    Await.result(result, Duration.Inf)
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + " millis")
    result
  }

}
