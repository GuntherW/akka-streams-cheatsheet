package de.wittig.akka.streams

import java.nio.charset.StandardCharsets
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.util.ByteString

/**
  * @see https://markatta.com/codemonkey/posts/akka-daytime-server/
  *
  *  Nach dem Start mit 'sbt run'
  *  kann der Server mit 'nc localhost 55555' aufgerufen werden.
  */
object TcpServer {

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("daytime")
    implicit val mat    = ActorMaterializer()

    val host = "localhost"
    val port = 55555

    Tcp().bind(host, port).runForeach { incomingConnection =>
      system.log.info("New connection, client address {}:{}", incomingConnection.remoteAddress.getHostString, incomingConnection.remoteAddress.getPort)
      incomingConnection.handleWith(daytimeFlow)
    }
  }

  val dayTimeSource: Source[ByteString, NotUsed] =
    Source
      .single(())
      .map { _ =>
        ByteString(
          DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now()),
          StandardCharsets.US_ASCII
        )
      }

  val daytimeFlow: Flow[ByteString, ByteString, NotUsed] =
    Flow.fromSinkAndSourceCoupled(Sink.ignore, dayTimeSource)
}
