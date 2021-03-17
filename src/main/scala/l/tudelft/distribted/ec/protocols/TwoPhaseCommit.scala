package l.tudelft.distribted.ec.protocols

import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.NetworkState

import scala.collection.mutable


abstract case class SupervisorState() extends Transaction

// State of supervisor in which it is counting how many agents agreed with the transaction.
case class ReceivingReadyState(id: String, receivedAddresses: mutable.HashSet[String]) extends Transaction

// State of cohort after receiving a PREPARE message and deciding to be ready
case class ReadyState(id: String) extends Transaction

// State of cohort or supervisor after having decided what should be done
case class CommitDecidedState(id: String) extends Transaction
case class AbortDecidedState(id: String) extends Transaction

// State of cohort or supervisor after having done what was decided
case class CommittedState(id: String) extends Transaction
case class AbortedState(id: String) extends Transaction

//TODO add timeouts
abstract class TwoPhaseCommit (
                          private val vertx: Vertx,
                          private val address: String,
                          private val database: HashMapDatabase,
                          private val timeout: Long = 5000L,
                          private val network: mutable.Map[String, NetworkState] = new mutable.HashMap[String, NetworkState]()
                        ) extends Protocol(vertx, address, database, timeout, network) {

  protected val stateManager = new TransactionStateManager(address)

  /**
   * Transaction is requested by an external party.
   * This agent will act as the supervisor for this transaction.
   * First step is to send to all cohorts to prepare, next step is handled by `handleProtocolMessage`
   * @param transaction the transaction to carry out.
   * TODO how is the transaction ID generated?
   */
  override def requestTransaction(transaction: Transaction): Unit = {
    // In this case this node is the supervisor
    if (stateManager.stateExists(transaction.id)) {
      // TODO perhaps do something with the old transaction? Queue?
    }

    stateManager.createState(transaction.id, address, ReceivingReadyState(transaction.id, mutable.HashSet()))
    sendToCohortExpectingReply(TransactionPrepareRequest(address, transaction.id), reply => {
      if (reply.succeeded()) {
        reply.result().body().toJsonObject.mapTo(classOf[ProtocolMessage]) match {
          case TransactionReadyResponse(sender, id, _) => handleReadyResponse(reply.result(), sender, id)
          case TransactionAbortResponse(sender, id, _) => handleAbortResponse(reply.result(), sender, id)
          case _ => // TODO ABORT
        }
      } else {
        // TODO Time-out or other failure
      }
    })
  }

  /**
   * Message has come in from one of the other cohorts.
   * This function decides which function should handle it.
   * Both supervisor- and supervised- targeted messages will come in,
   * meaning in some messages the agent should act as supervisor,
   * and in other messages as supervised.
   *
   * @param message raw message received.
   * @param protocolMessage parsed message, matched to one of its subclasses.
   */
  override def handleProtocolMessage(message: Message[Buffer], protocolMessage: ProtocolMessage): Unit = protocolMessage match {
    case TransactionPrepareRequest(sender, id, _) => handlePrepareRequest(message, sender, id)
    case TransactionAbortResponse(sender, id, _) => handleAbortResponse(message, sender, id)
    case TransactionCommitRequest(sender, id, _) => handleCommitRequest(message, sender, id)
  }

  /**
   * A supervisor has asked this agent to prepare for a request.
   * Report back to the supervisor if this is possible.
   * @param sender the network address of the sender of this message.
   * @param id the id of the transaction to prepare for.
   */
  def handlePrepareRequest(message: Message[Buffer], sender: String, id: String): Unit = {
    if (stateManager.stateExists(id)) {
      val supervisor = stateManager.getSupervisor(id)
      if (sender != supervisor) {
        // Some faulty sender is trying to interfere with this transaction
        // Drop this request, and do not respond
        return
      } else {
        //TODO what to do if this transaction was already started by this supervisor?
        return
      }
    }

    //TODO check if this change is possible
    val ready = true
    if (ready) {
      // Remember the READY response is being sent to the supervisor
      stateManager.createState(id, sender, ReadyState(id))
      replyToMessage(message, TransactionReadyResponse(sender, id))
    } else {
      stateManager.createState(id, sender, AbortedState(id))
      replyToMessage(message, TransactionAbortResponse(address, id))
    }
  }

  /**
   * Handles the case that a READY package was sent to this agent.
   * Function only acts on this if this agent thinks it is the supervisor for this transaction.
   * If the number of READY's received from distinct senders
   * is equal to the number of agents in the network, this supervisor will commit.
   *
   * @param sender the network address of the sender of this message.
   * @param id the id of the transaction the sender sent READY for.
   */
  def handleReadyResponse(message: Message[Buffer], sender: String, id: String): Unit = {
    if (!stateManager.stateExists(id)) {
      // Probably supervised this earlier but was deleted due to an abort
      return
    }

    // Check if this agent is the supervising entity.
    // If not, drop this request and do not respond.
    if (stateManager.getSupervisor(id) != address) {
      return
    }

    stateManager.getState(id) match {
      case ReceivingReadyState(_, confirmedAddresses) =>
        confirmedAddresses += sender

        // TODO check if network.size is in- or excluding this node
        if (confirmedAddresses.size < network.size) {
          stateManager.updateState(id, ReceivingReadyState(id, confirmedAddresses))
          return
        }

        // Global decision has been made, transfer commit decision to all cohorts
        commitTransaction(id)
      case _ =>
      // A READY was received by the supervisor, but the supervisor is not expecting it.
      // Drop the package.
      // TODO log that a wrong package was received
    }
  }

  /**
   * Handle when an ABORT message is sent to this agent.
   * Function only acts when this agent is the supervisor of the transaction,
   * or when the message is from the supervisor of this transaction.
   *
   * @param sender the sender of the ABORT message.
   * @param id the transaction the ABORT message was sent about.
   */
  def handleAbortResponse(message: Message[Buffer], sender: String, id: String): Unit = {
    if (!stateManager.stateExists(id)) {
      // No state with this id, so abort what?
      //TODO log that a malicious package was found.
      return
    }

    val supervisor = stateManager.getSupervisor(id)
    if (supervisor == address || supervisor == sender) {
      // This agent is the supervisor of this transaction, or the message was from the supervisor
      abortTransaction(id)
    }
    // Else the abort came from an agent who sent the message for redundancy
  }

  /**
   * Handle committing of a transaction to the database.
   * If this message is from the supervisor, repeat the message to everyone
   * and update the database.
   *
   * @param sender the sender of the commit request.
   * @param id the id of the transaction the commit was about.
   */
  def handleCommitRequest(message: Message[Buffer], sender: String, id: String): Unit = {
    if (!stateManager.stateExists(id)) {
      // No state with this id, so do not respond.
      //TODO log that a malicious package was found
      return
    }

    val state = stateManager.getState(id)
    if (state != ReadyState(id)) {
      // This state is not yet in the ready state.
      //TODO log that a malicious package was found.
      return
    }

    val supervisor = stateManager.getSupervisor(id)
    if (supervisor == sender && supervisor != address) {
      commitTransaction(id)
    }
  }

  /**
   * Commit the transaction to the database.
   *
   * @param id the id of the transaction to commit.
   */
  def commitTransaction(id: String): Unit = {
    stateManager.updateState(id, CommitDecidedState(id))
    //TODO commit the DB change
    stateManager.updateState(id, CommittedState(id))
  }

  /**
   * Abort the transaction.
   *
   * @param id the id of the transaction to abort.
   */
  def abortTransaction(id: String): Unit = {
    // Update everyone else, and stop tracking this.
    stateManager.updateState(id, AbortDecidedState(id))
    // TODO abort the transaction in the DB
    stateManager.updateState(id, AbortedState(id))
  }
}