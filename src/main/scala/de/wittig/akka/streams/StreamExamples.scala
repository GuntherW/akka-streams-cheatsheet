package de.wittig.akka.streams

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.{Done, NotUsed}

import scala.concurrent._
import scala.concurrent.duration._

/**
  * https://www.beyondthelines.net/computing/akka-streams-patterns/
  */
object StreamExamples extends App {

  implicit private val system: ActorSystem             = ActorSystem("Akka-Streams-Patterns")
  implicit private val materializer: ActorMaterializer = ActorMaterializer()
  import system.dispatcher

//  firstExampleRunnableGraph
  // time(serial)
  //time(serialAndParallel)
//  time(simpleSource)
//  time(flatteningAStream)
//  time(flattenAStream)
//  time(usingScalaStreams  )
//  time(flatMapConcats      )
//  time(flatMapMerge)
//  time(batchingGrouped)
//  time(batchingGroupedWithing)
//  time(writeBatchToDatabase)
//  time(writeBatchToDatabaseUnordered)
//  time(viaFlow)
//  time(viaFlowParalell)
//  time(throttling)
//  time(idleOut)
//  time(errorHandlingRestart)
//  time(combine())
  time(mapSync())
  time(mapAsync())

  system.terminate()

  def firstExampleRunnableGraph(): Unit = {

    val source                       = Source(1 to 10)
    val sink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)

    // connect the Source to the Sink, obtaining a RunnableGraph
    val runnable: RunnableGraph[Future[Int]] = source.toMat(sink)(Keep.right)

    // materialize the flow and get the value of the FoldSink
    val sum1: Future[Int] = runnable.run()
    println(Await.result(sum1, Duration.Inf))

    // Um diesen Standardfall einfacher zu machen, kann man eine Source direkt mit einer Sink verbinden.
    // materialize the flow, getting the Sinks materialized value
    val sum2: Future[Int] = source.runWith(sink)
    println(Await.result(sum2, Duration.Inf))
  }

  def serial: Future[Done] =
    Source(List.tabulate(100000)(identity))
      .map(_ + 1)
      .map(_ * 2)
      .runWith(Sink.ignore)

  def serialAndParallel =
    Source(List.tabulate(100000)(identity))
      .map(_ + 1)
      .async
      .map(_ * 2)
      .runWith(Sink.ignore)

  def simpleSource: Future[Done] =
    Source(1 to 100)
      .runForeach(i => println(i))

  def flatteningAStream =
    Source('A' to 'E')
      .mapConcat(letter => (1 to 3).map(index => s"$letter$index"))
      .runForeach(println)

  def flattenAStream =
    // Zum Beispiel, wenn von der DB ein Future[Iterable[Row]] kommt
    Source
      .fromFuture(Future.successful(1 to 10))
      .mapConcat(identity)
      .runForeach(println)

  def usingScalaStreams =
    Source
      .fromFuture(Future.successful(Stream.range(1, 10)))
      .flatMapConcat(Source.apply)
      .runForeach(println)

  def flatMapConcats =
    Source('A' to 'E')
      .runForeach(println)

  // flatMapConcat in Parallel
  def flatMapMerge =
    Source('A' to 'E')
      .flatMapMerge(5, letter => Source(1 to 3).map(index => s"$letter$index"))
      .runForeach(println)

  // With grouped we can process the stream as batches.
  def batchingGrouped =
    Source(1 to 100)
      .grouped(10)
//      .mapConcat(identity) // Flattening a stream of sequences
      .runForeach(println)

  def batchingGroupedWithing =
    Source
      .tick(0.millis, 10.millis, ())
      .groupedWithin(100, 100.millis)
      .map { batch =>
        println(s"Processing batch of ${batch.size} elements")
        batch
      }
      .runWith(Sink.ignore)

  def writeBatchToDatabase =
    Source(1 to 1000000)
      .grouped(10)
      .mapAsync(10)(writeToDatabase)
      .runWith(Sink.ignore)

  def writeBatchToDatabaseUnordered =
    Source(1 to 1000000)
      .grouped(10)
      .mapAsyncUnordered(10)(writeToDatabase)
      .runWith(Sink.ignore)

  def viaFlow = {
    def stage(name: String): Flow[Int, Int, NotUsed] =
      Flow[Int].map { index =>
        println(s"Stage $name processing $index by ${Thread.currentThread().getName}")
        index
      }
    Source(1 to 1000000)
      .via(stage("A"))
      .via(stage("B"))
      .via(stage("C"))
      .runWith(Sink.ignore)
  }

  def viaFlowParalell = {
    def stage(name: String): Flow[Int, Int, NotUsed] =
      Flow[Int].map { index =>
        println(s"Stage $name processing $index by ${Thread.currentThread().getName}")
        index
      }
    Source(1 to 1000000)
      .via(stage("A"))
      .async
      .via(stage("B"))
      .async
      .via(stage("C"))
      .async
      .runWith(Sink.ignore)
  }

  def throttling =
    Source(1 to 1000)
      .grouped(10)
      .throttle(elements = 10, per = 1.second, maximumBurst = 10, ThrottleMode.shaping)
      .mapAsync(10)(writeToDatabase)
      .runWith(Sink.ignore)

  def idleOut =
    Source
      .tick(0.millis, 15 seconds, ())
      .idleTimeout(10.seconds)
      .runWith(Sink.ignore)
      .recover {
        case _: TimeoutException =>
          println("No messages received for 10 seconds")
      }

  def errorHandlingRestart =
    Source(1 to 5)
      .map {
        case 3 => throw new Exception("3 is bad")
        case n => n
      }
      .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
      .runForeach(println)

  def filter() = {
    Source(1 to 20)
      .filter(_ % 2 == 0)
      .runWith(Sink.foreach(println))
  }

  def combine(): Future[Done] = {

    val s1 = Source(List(1, 2, 3, 7, 10))
    val s2 = Source(List(10, 20, 30, 15, 19))

    val filterFlow: Flow[Int, Int, NotUsed]      = Flow[Int].filter(_ > 2)
    val multiplyFlow: Flow[Int, Int, NotUsed]    = Flow[Int].map(_ * 2)
    val toStringFlow: Flow[Int, String, NotUsed] = Flow[Int].map(_.toString)

    Source
      .combine(s1, s2)(Concat(_))
      .via(filterFlow)
      .via(multiplyFlow)
      .via(toStringFlow)
      .runWith(Sink.foreach(println))
  }

  case class User(id: Int, name: String, age: Int)

  def mapSync() = {

    def querySync(id: Int): User = {
      println(s"start query - $id")
      Thread.sleep(1000)
      println(s"finish query - $id")
      User(1, s"user$id", 30)
    }

    val source                                  = Source(List(3, 2, 5, 7, 8))
    val queryFlowSync: Flow[Int, User, NotUsed] = Flow[Int].map(i => querySync(i))
    val nameFlow: Flow[User, String, NotUsed]   = Flow[User].map(u => u.name)

    source
      .via(queryFlowSync)
      .via(nameFlow)
      .runWith(Sink.foreach(println))
  }

  def mapAsync() = {

    def queryAsync(id: Int): Future[User] = Future {
      println(s"start query - $id")
      Thread.sleep(1000)
      println(s"finish query - $id")
      User(1, s"user$id", 30)
    }

    val source                                  = Source(List(3, 2, 5, 7, 8))
    val queryFlowSync: Flow[Int, User, NotUsed] = Flow[Int].mapAsync(5)(i => queryAsync(i))
    val nameFlow: Flow[User, String, NotUsed]   = Flow[User].map(u => u.name)

    source
      .via(queryFlowSync)
      .via(nameFlow)
      .runWith(Sink.foreach(println))
  }
  private def writeToDatabase[T](batch: Seq[T]): Future[Unit] = Future {
    println(s"Thread: ${Thread.currentThread().getName} - Writing batch of $batch to database")
  }
}
