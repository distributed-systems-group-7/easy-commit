package l.tudelft.distribted.ec.protocols

import scala.collection.mutable

/**
 * Tracks the state and supervisor of each transaction.
 * Makes sure there are no discrepancies between its to fields.
 * TODO Temporary fix until we fix a real log.
 *
 * @param address the address of the agent creating this manager.
 */
class TransactionStateManager (private val address: String){
  // Link between a transition ID and its supervisor
  private val supervisorMap = new mutable.HashMap[String, String]()

  // Link between a transition id and the state this cohort is in
  private val stateMap = new mutable.HashMap[String, Transaction]()

  /**
   * Create a new state to track the state of a transaction.
   * @param id the id of the transaction to track.
   * @param supervisor the network address of the supervisor.
   */
  def createState(id: String, supervisor: String, state: Transaction): Unit = {
    supervisorMap.put(id, supervisor)
    stateMap.put(id, state)
  }

  /**
   * Update the state some transaction is in.
   * This function should be used instead of changing stateMap directly.
   * @param id the id of the transaction to update the state of.
   * @param state the new state of the transaction.
   */
  def updateState(id: String, state: Transaction): Unit = {
    if (!stateExists(id)) {
      throw new IllegalStateException("Cannot add a state when it was not created.")
    }
    stateMap.put(id, state)
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
