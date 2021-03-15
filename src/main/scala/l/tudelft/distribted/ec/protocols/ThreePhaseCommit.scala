package l.tudelft.distribted.ec.protocols

import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.NetworkState
import l.tudelft.distribted.ec.protocols.ProtocolState.ProtocolState
import l.tudelft.distribted.ec.protocols.{PrepareTransactionMessage,VoteCommitMessage,VoteAbortMessage,GlobalCommitMessage,GlobalAbortMessage,GlobalCommitAckMessage,GlobalAbortAckMessage}

import scala.collection.mutable

case class GlobalPreCommitMessage(sender: String, transactionId: String, `type`:String = "protocol.threephasecommit.globalprecommit") extends ProtocolMessage()
case class GlobalPreCommitAckMessage(sender: String, transactionId: String, `type`:String = "protocol.threephasecommit.globalprecommitack") extends ProtocolMessage()

  class ThreePhaseCommit(
                      private val vertx: Vertx,
                      private val address: String,
                      private val database: HashMapDatabase,
                      private val timeout: Long = 5000L,
                      private val network: mutable.Map[String, NetworkState] = new mutable.HashMap[String, NetworkState]()
                    ) extends Protocol(vertx, address, database, timeout, network) {

  private val states: mutable.Map[String, ProtocolState] = new mutable.HashMap[String, ProtocolState]()

  override def requestTransaction(transaction: Transaction): Unit = {
    val numberOfCohorts = network.size - 1
    var numberOfCommits = 0
    var numberOfCommitAcks = 0
    var numberOfAbortAcks = 0
    var abortFlag = false

    states += ((transaction.id, ProtocolState.INITIAL))
    performTransaction(transaction)
    sendToCohortExpectingReply(PrepareTransactionMessage(address, transaction), (response: AsyncResult[Message[Buffer]]) => {
      if (response.succeeded()) {
        response.result().body().toJsonObject.mapTo(classOf[ProtocolMessage]) match {

          case VoteCommitMessage(sender, transactionId, _) =>
            numberOfCommits += 1
            if (numberOfCommits == numberOfCohorts) {
              states += ((transactionId, ProtocolState.COMMIT))
              sendToCohortExpectingReply(GlobalCommitMessage(address, transactionId), (response: AsyncResult[Message[Buffer]]) => {
                if (response.succeeded()) {
                  response.result().body().toJsonObject.mapTo(classOf[ProtocolMessage]) match {

                    case GlobalCommitAckMessage(sender, transactionId, _) =>
                      numberOfCommitAcks += 1
                      if (numberOfCommitAcks == numberOfCohorts) {
                        states += ((transactionId, ProtocolState.CLOSED))
                        println("Coordinator done.")
                      }
                  }
                }
              })
            }

          case VoteAbortMessage(sender, transactionId, _) =>
            states += ((transactionId, ProtocolState.ABORT))
            sendToCohortExpectingReply(GlobalAbortMessage(address, transactionId), (response: AsyncResult[Message[Buffer]]) => {
              if (response.succeeded()) {
                response.result().body().toJsonObject.mapTo(classOf[ProtocolMessage]) match {

                  case GlobalAbortAckMessage(sender, transactionId, _) =>
                    numberOfAbortAcks += 1
                    if (numberOfAbortAcks == numberOfCohorts) {
                      states += ((transactionId, ProtocolState.CLOSED))
                      println("Coordinator abort.")
                    }
                }
              }
            })

        }
      }
      else if (response.failed()) {
        states += ((transaction.id, ProtocolState.ABORT))
        sendToCohortExpectingReply(GlobalAbortMessage(address, transaction.id), (response: AsyncResult[Message[Buffer]]) => {
          if (response.succeeded()) {
            response.result().body().toJsonObject.mapTo(classOf[ProtocolMessage]) match {

              case GlobalAbortAckMessage(sender, transactionId, _) =>
                numberOfAbortAcks += 1
                if (numberOfAbortAcks == numberOfCohorts) {
                  states += ((transactionId, ProtocolState.CLOSED))
                  println("Coordinator abort.")
                }
            }
          }
        })
      }
    })
  }

  override def handleProtocolMessage(message: Message[Buffer], protocolMessage: ProtocolMessage): Unit = {
    protocolMessage match {
      case PrepareTransactionMessage(sender, transaction, _) =>
        states += ((transaction.id, ProtocolState.INITIAL))
        try {
          states += ((transaction.id, ProtocolState.READY))
          performTransaction(transaction)
          message.reply(Json.encodeToBuffer(VoteCommitMessage(address, transaction.id)))
        } catch {
          case _ =>
            states += ((transaction.id, ProtocolState.ABORT))
            message.reply(Json.encodeToBuffer(VoteAbortMessage(address, transaction.id)))
        }

      case GlobalCommitMessage(sender, transactionId, _) =>
        states += ((transactionId, ProtocolState.COMMIT))
        message.reply(Json.encodeToBuffer(GlobalCommitAckMessage(address, transactionId)))
        println("Cohort done.")

      case GlobalAbortMessage(sender, transactionId, _) =>
        states += ((transactionId, ProtocolState.ABORT))
        // TODO revert possibly already performed transactions
        message.reply(Json.encodeToBuffer(GlobalAbortAckMessage(address, transactionId)))
        println("Cohort abort.")
    }
  }

  def handleAbort(): Unit = {

  }
}