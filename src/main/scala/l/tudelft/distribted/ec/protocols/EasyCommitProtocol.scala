package l.tudelft.distribted.ec.protocols

import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.NetworkState

import scala.collection.mutable


abstract case class SupervisorState() extends Transaction

// Tracks from which cohorts this supervisor has received a READY response
case class ReceivingReadyState(id: String, receivedAddresses: mutable.HashSet[String]) extends Transaction

case class ReadyState(id: String) extends Transaction
case class CommittedState(id: String) extends Transaction
case class AbortedState(id: String) extends Transaction


class EasyCommitProtocol(
    private val vertx: Vertx,
    private val address: String,
    private val database: HashMapDatabase,
    private val timeout: Long = 5000L,
    private val network: mutable.Map[String, NetworkState] = new mutable.HashMap[String, NetworkState]()
  ) extends Protocol(vertx, address, database, timeout, network) {

  private val stateManager = new TransactionStateManager(address)
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

    stateManager.createState(transaction.id, address);
    sendToCohort(TransactionPrepareRequest(address, transaction.id))
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
    case TransactionPrepareRequest(sender, id, _) => handlePrepareRequest(sender, id)
    case TransactionReadyResponse(sender, id, _) => handleReadyResponse(sender, id)
    case TransactionAbortResponse(sender, id, _) => handleAbortResponse(sender, id)
    case TransactionCommitRequest(sender, id, _) => handleCommitRequest(sender, id)
  }

  /**
   * A supervisor has asked this agent to prepare for a request.
   * Report back to the supervisor if this is possible.
   * @param sender the network address of the sender of this message.
   * @param id the id of the transaction to prepare for.
   */
  def handlePrepareRequest(sender: String, id: String): Unit = {
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
    stateManager.createState(id, sender)
    if (ready) {
      // Remember the READY response is being sent to the supervisor
      sendToAddress(sender, TransactionReadyResponse(sender, id))
    } else {
      stateManager.updateState(id, AbortedState(id))
      sendToAddress(sender, TransactionAbortResponse(address, id))
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
  def handleReadyResponse(sender: String, id: String): Unit = {
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
      case ReceivingReadyState(_, confirmedAddresses) => {
        confirmedAddresses += sender

        // TODO check if network.size is in- or excluding this node
        if (confirmedAddresses.size < network.size) {
          stateManager.updateState(id, ReceivingReadyState(id, confirmedAddresses))
          return
        }

        // Global decision has been made, transfer commit decision to all cohorts
        sendToCohort(TransactionCommitRequest(address, id))
        stateManager.updateState(id, CommittedState(id))
      }
      case _ => {
        // A READY was received by the supervisor, but the supervisor is not expecting it.
        // Drop the package.
        // TODO log that a wrong package was received
      }
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
  def handleAbortResponse(sender: String, id: String): Unit = {
    if (!stateManager.stateExists(id)) {
      // No state with this id, so abort what?
      //TODO log that a malicious package was found.
      return
    }

    val supervisor = stateManager.getSupervisor(id)
    if (supervisor == address || supervisor == sender) {
      // This agent is the supervisor of this transaction, or the message was from the supervisor
      // Update everyone else, and stop tracking this.
      sendToCohort(TransactionAbortResponse(address, id))
      stateManager.updateState(id, AbortedState(id))
    }
    // Else the abort came from an agent who sent the message for redundancy
  }

  /**
   * Handle committing of a transaction to the database.
   * If this message is from the supervisor, repeat the message to everyone
   * and update the database.
   *
   * @param sender
   * @param id
   */
  def handleCommitRequest(sender: String, id: String): Unit = {
    if (!stateManager.stateExists(id)) {
      // No state with this id, so do not respond.
      //TODO log that a malicious package was found
      return
    }
    val supervisor = stateManager.getSupervisor(id)
    if (supervisor == sender) {
      stateManager.updateState(id, CommittedState(id))
      sendToCohort(TransactionCommitRequest(address, id))
      // TODO update the DB
    }
  }

}

/**
 * Tracks the state and supervisor of each transaction.
 * Makes sure there are no discrepancies between its to fields.
 * TODO Temporary fix until we fix a real log.
 * @param address the address of the agent creating this manager.
 */
class TransactionStateManager (private val address: String){
  // Link between a transition ID and its supervisor
  private val supervisorMap = new mutable.HashMap[String, String]()

  // Link between a transition id and the state this cohort is in
  private val stateMap = new mutable.HashMap[String, Transaction]()

  /**
   * Create a new state to track the state of a transaction.
   * If the address is equal to the address of the creator of this manager,
   * a `ReceivingReadyState` will be created as state, indicating this agent is listening for READY messages.
   * If the address is unequal, then a `ReadyState` will be created,
   * indicating this agent has responded to a PREPARE message from a supervisor.
   *
   * @param id the id of the transaction to track.
   * @param supervisor the network address of the supervisor.
   */
  def createState(id: String, supervisor: String): Unit = {
    supervisorMap.put(id, supervisor)
    if (supervisor == address) {
      stateMap.put(id, ReceivingReadyState(id, 0))
    } else {
      stateMap.put(id, ReadyState(id))
    }
  }

  /**
   * Update the state some transaction is in.
   * This function should be used instead of changing stateMap directly.
   * @param id the id of the transaction to update the state of.
   * @param state the new state of the transaction.
   */
  def updateState(id: String, state: Transaction): Unit = {
    stateMap.put(id, state)
  }

  /**
   * Stop tracking some state.
   * For example if committed or aborted.
   * TODO can be deleted when we change to an actual log.
   * @param id representing the transaction to delete.
   */
  def removeState(id: String): Unit = {
    supervisorMap.remove(id)
    stateMap.remove(id)
  }

  /**
   * Get the supervisor of a transaction.
   * CAUTION: Will throw an error if transaction is not being tracked.
   * Use `stateExists` to check whether a record with this ID exists.
   * @param id the id of the transaction to get the supervisor of.
   * @return the network address of the supervisor of this transaction.
   */
  def getSupervisor(id: String): String = {
    supervisorMap(id)
  }

  /**
   * Get the state of a transaction.
   * CAUTION: Will throw an error if transaction is not being tracked.
   * Use `stateExists` to check whether a record with this ID exists.
   * @param id the id of the transaction to get the state of.
   * @return the state this transaction is in.
   */
  def getState(id: String): Transaction = {
    stateMap(id)
  }

  /**
   * Check whether a given transaction is being tracked by this class.
   * If there is a discrepancy between stateMap and supervisorMap, false is returned.
   * @param id the id of the transaction to check if it exists.
   * @return true if this transaction is being tracked, false else.
   */
  def stateExists(id: String): Boolean = {
    val superExists = supervisorMap.contains(id)
    val stateExists = stateMap.contains(id)

    if (superExists && stateExists) {
      true
    } else {
      // Illegal state: discrepancy between supervisorMap and stateMap
      // Fix it by deleting the existing key
      if (superExists) {
        supervisorMap.remove(id)
      } else {
        stateMap.remove(id)
      }
      false
    }
  }
}
