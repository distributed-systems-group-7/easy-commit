package l.tudelft.distribted.ec.protocols

import io.vertx.core.buffer.Buffer
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.Message
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.NetworkState

import scala.collection.mutable

case class ECReadyState(commitsReceived: mutable.HashSet[String], abortsReceived: mutable.HashSet[String], id: String) extends PhaseState;

/**
 * Easy commit protocol, implemented by example of paper by Gupta & Sadoghi (2018).
 *
 * @param vertx the message bus over which to communicate.
 * @param address the network address of this agent.
 * @param database the database on which to execute the transactions.
 * @param timeout the timeout of no response of a cohort or supervisor.
 * @param network state mapping of how reachable the network is.
 */
class EasyCommitProtocol(
    private val vertx: Vertx,
    private val address: String,
    private val database: HashMapDatabase,
    private val timeout: Long = 5000L,
    private val network: mutable.Map[String, NetworkState] = new mutable.HashMap[String, NetworkState]()
  ) extends TwoPhaseCommit(vertx, address, database, timeout, network) {

  /**
   * A supervisor has asked this agent to prepare for a request.
   * Report back to the supervisor if this is possible.
   *
   * @param message     raw form of the message that was sent.
   * @param sender      the network address of the sender of the message.
   * @param transaction the transaction to prepare for.
   */
  override def handlePrepareRequest(message: Message[Buffer], sender: String, transaction: Transaction): Unit = {
    val id = transaction.id
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

    if (transactionPossible(transaction)) {
      // Remember the READY response is being sent to the supervisor
      // Keep track of how many commit or aborts were received (before receiving from supervisor)
      stateManager.createState(transaction, sender, ECReadyState(
        new mutable.HashSet[String](),
        new mutable.HashSet[String](),
        id
      ))
      replyToMessage(message, TransactionReadyResponse(address, id))
    } else {
      stateManager.createState(transaction, sender, AbortedState(id))
      replyToMessage(message, TransactionAbortResponse(address, id))
    }
  }

  /**
   * Handle committing of a transaction to the database.
   * If this message is from the supervisor, repeat the message to everyone
   * and update the database.
   *
   * @param message raw form of the message that was sent. Used to directly reply.
   * @param sender  the sender of the commit request.
   * @param id      the id of the transaction the commit was about.
   */
  override def handleCommitRequest(message: Message[Buffer], sender: String, id: String): Unit = {
    if (!stateManager.stateExists(id)) {
      // No state with this id, so do not respond.
      //TODO log that a malicious package was found
      return
    }

    val supervisor = stateManager.getSupervisor(id)
    stateManager.getState(id) match {
      case ECReadyState(commitsReceived, abortsReceived, _) => {
        if (supervisor == sender && supervisor != address) {
          commitTransaction(id)
        } else if (supervisor != sender) {
          // This was a relay message from another cohort.
          commitsReceived += sender
          stateManager.updateState(id, ECReadyState(commitsReceived, abortsReceived, id))
          if (commitsReceived.size > 0.5 * network.size) { //TODO AND TIMEOUT RECEIVED
            commitTransaction(id)
          }
        }
      }
      case _ =>
        // This state is not yet in the ready case
        // TODO log that a malicious package was found.
    }
  }

  /**
   * Handle when an ABORT message is sent to this agent.
   * Function only acts when this agent is the supervisor of the transaction,
   * or when the message is from the supervisor of this transaction.
   *
   * @param message raw form of the message that was sent. Used to directly reply.
   * @param sender  the sender of the ABORT message.
   * @param id      the transaction the ABORT message was sent about.
   */
  override def handleAbortResponse(message: Message[Buffer], sender: String, id: String): Unit = {
    if (!stateManager.stateExists(id)) {
      // No state with this id, so abort what?
      //TODO log that a malicious package was found.
      return
    }

    val supervisor = stateManager.getSupervisor(id)
    if (supervisor == address || supervisor == sender) {
      // This agent is the supervisor of this transaction, or the message was from the supervisor
      abortTransaction(id)
    } else {
      // The abort came from an agent who sent the message for redundancy
      stateManager.getState(id) match {
        case ECReadyState(commitsReceived, abortsReceived, _) => {
          abortsReceived += sender
          stateManager.updateState(id, ECReadyState(commitsReceived, abortsReceived, id))
          if (abortsReceived.size > network.size) { // TODO AND TIMEOUT RECEIVED
            abortTransaction()
          }
        }
      }
    }
    // Else the abort came from an agent who sent the message for redundancy
  }

  /**
   * Commit to the database. Let everyone know this is being done.
   *
   * @param id the id of the transaction to commit.
   */
  override def commitTransaction(id: String): Unit = {
    // Update everyone else, and commit to DB.
    stateManager.updateState(id, CommitDecidedState(id))
    sendToCohort(TransactionCommitRequest(address, id))
    //TODO commit the DB change
    stateManager.updateState(id, CommittedState(id))
  }


  /**
   * Abort the transaction to the DB. Let everyone know this is being done.
   *
   * @param id the id of the transaction to abort.
   */
  override def abortTransaction(id: String): Unit = {
    // Update everyone else, and stop tracking this.
    stateManager.updateState(id, AbortDecidedState(id))
    sendToCohort(TransactionAbortResponse(address, id))
    // TODO abort the transaction in the DB
    stateManager.updateState(id, AbortedState(id))
  }
}