package l.tudelft.distribted.ec.protocols

import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.NetworkState
import l.tudelft.distribted.ec.protocols.ProtocolState.ProtocolState

import scala.collection.mutable

case class PrepareTransactionMessage(sender: String, transaction: Transaction, `type`:String = "protocol.twophasecommit.prepare") extends ProtocolMessage()

case class VoteCommitMessage(sender: String, transactionId: String, `type`:String = "protocol.twophasecommit.votecommit") extends ProtocolMessage()

case class VoteAbortMessage(sender: String, transactionId: String, `type`:String = "protocol.twophasecommit.voteabort") extends ProtocolMessage()

case class GlobalCommitMessage(sender: String, transactionId: String, `type`:String = "protocol.twophasecommit.globalcommit") extends ProtocolMessage()

case class GlobalAbortMessage(sender: String, transactionId: String, `type`:String = "protocol.twophasecommit.globalabort") extends ProtocolMessage()

case class GlobalCommitAckMessage(sender: String, transactionId: String, `type`:String = "protocol.twophasecommit.globalcommitack") extends ProtocolMessage()

case class GlobalAbortAckMessage(sender: String, transactionId: String, `type`:String = "protocol.twophasecommit.globalabortack") extends ProtocolMessage()

class TwoPhaseCommit(
                       private val vertx: Vertx,
                       private val address: String,
                       private val database: HashMapDatabase,
                       private val timeout: Long = 5000L,
                       private val network: mutable.Map[String, NetworkState] = new mutable.HashMap[String, NetworkState]()
                     ) extends Protocol(vertx, address, database, timeout, network) {

  private val states: mutable.Map[String, ProtocolState] = new mutable.HashMap[String, ProtocolState]()

  override def requestTransaction(transaction: Transaction): Unit = {
    val numberOfCohorts = network.size
    var numberOfCommits = 0
    var numberOfCommitAcks = 0
    var numberOfAbortAcks = 0

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
                      }
                  }
                }
              })
            }
            else {
              // timeout handling?
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
                    }
                }
              }
            })

        }
      }
      else if (response.failed()) {
        states += ((transaction.id, ProtocolState.ABORT))
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
          sendToCohort(VoteCommitMessage(address, transaction.id))
        } catch {
          case _ =>
            states += ((transaction.id, ProtocolState.ABORT))
            sendToCohort(VoteAbortMessage(address, transaction.id))
        }

      case GlobalCommitMessage(sender, transactionId, _) =>
        states += ((transactionId, ProtocolState.COMMIT))
        sendToCohort(GlobalCommitAckMessage(address, transactionId))

      case GlobalAbortMessage(sender, transactionId, _) =>
        states += ((transactionId, ProtocolState.ABORT))
        // TODO revert possibly already performed transactions
        sendToCohort(GlobalAbortAckMessage(address, transactionId))
    }
  }
}