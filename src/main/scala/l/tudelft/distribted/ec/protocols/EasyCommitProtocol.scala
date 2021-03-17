package l.tudelft.distribted.ec.protocols

import io.vertx.scala.core.Vertx
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.NetworkState

import scala.collection.mutable

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