package l.tudelft.distribted.ec

import io.vertx.lang.scala.ScalaVerticle
import io.vertx.scala.ext.web.Router
import l.tudelft.distribted.ec.protocols.ExampleProtocol

import scala.collection.mutable
import scala.concurrent.Future

class WebVerticle(val name: String, val port: Int) extends ScalaVerticle {
  override def startFuture(): Future[_] = {
    val router = Router.router(vertx)

    vertx
      .createHttpServer()
      .listenFuture(port, "0.0.0.0")
  }
}
