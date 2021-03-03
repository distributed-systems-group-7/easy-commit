package l.tudelft.distribted.ec.protocols

import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.NetworkState

import scala.collection.mutable

case class PerformTransactionMessage(sender: String, transaction: Transaction, `type`:String = "protocol.example") extends ProtocolMessage()

class ExampleProtocol(
                       private val vertx: Vertx,
                       private val address: String,
                       private val database: HashMapDatabase,
                       private val timeout: Long = 5000L,
                       private val network: mutable.Map[String, NetworkState] = new mutable.HashMap[String, NetworkState]()
                     ) extends Protocol(vertx, address, database, timeout, network) {
  override def requestTransaction(transaction: Transaction): Unit = {
    performTransaction(transaction)
    sendToCohort(PerformTransactionMessage(address, transaction))
  }

  override def handleProtocolMessage(message: Message[Buffer], protocolMessage: ProtocolMessage): Unit = {
    protocolMessage match {
      case PerformTransactionMessage(_, transaction, _) => performTransaction(transaction)
    }
  }
}
