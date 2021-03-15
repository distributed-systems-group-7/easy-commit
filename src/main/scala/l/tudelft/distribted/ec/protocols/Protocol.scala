package l.tudelft.distribted.ec.protocols

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.lang.scala.json.JsonObject
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.{DeliveryOptions, EventBus, Message}
import l.tudelft.distribted.ec.HashMapDatabase
import l.tudelft.distribted.ec.protocols.NetworkState.{NetworkState, READY}

import scala.collection.mutable

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[RemoveDataTransaction], name = "transaction.remove"),
  new Type(value = classOf[StoreDataTransaction], name = "transaction.store"),
))
trait Transaction {
  def id: String
}

case class RemoveDataTransaction(id: String, keyToRemove: String, `type`: String = "transaction.remove") extends Transaction

case class StoreDataTransaction(id: String, keyToStore: String, data: java.util.Map[String, AnyRef], `type`: String = "transaction.store") extends Transaction

object NetworkState extends Enumeration {
  type NetworkState = Value
  val READY = Value
}

object ProtocolState extends Enumeration {
  type ProtocolState = Value
  val INITIAL, WAIT, READY, ABORT, COMMIT, CLOSED = Value
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[RequestNetwork], name = "request.network"),
  new Type(value = classOf[RespondNetwork], name = "response.network"),
  new Type(value = classOf[PerformTransactionMessage], name = "protocol.example"),
  new Type(value = classOf[PrepareTransactionMessage], name = "protocol.twophasecommit.prepare"),
  new Type(value = classOf[VoteCommitMessage], name = "protocol.twophasecommit.votecommit"),
  new Type(value = classOf[VoteAbortMessage], name = "protocol.twophasecommit.voteabort"),
  new Type(value = classOf[GlobalCommitMessage], name = "protocol.twophasecommit.globalcommit"),
  new Type(value = classOf[GlobalAbortMessage], name = "protocol.twophasecommit.globalabort"),
  new Type(value = classOf[GlobalCommitAckMessage], name = "protocol.twophasecommit.globalcommitack"),
  new Type(value = classOf[GlobalAbortAckMessage], name = "protocol.twophasecommit.globalabortack"),
  new Type(value = classOf[VoteCommitMessage], name = "protocol.threephasecommit.votecommit"),
  new Type(value = classOf[VoteAbortMessage], name = "protocol.threephasecommit.voteabort"),
  new Type(value = classOf[GlobalCommitMessage], name = "protocol.threephasecommit.globalcommit"),
  new Type(value = classOf[GlobalAbortMessage], name = "protocol.threephasecommit.globalabort"),
  new Type(value = classOf[GlobalCommitAckMessage], name = "protocol.threephasecommit.globalcommitack"),
  new Type(value = classOf[GlobalAbortAckMessage], name = "protocol.threephasecommit.globalabortack"),
))
trait ProtocolMessage {
  def sender: String

  def `type`: String
}


case class RequestNetwork(sender: String, state: NetworkState, `type`: String = "request.network") extends ProtocolMessage

case class RespondNetwork(sender: String, state: NetworkState, `type`: String = "response.network") extends ProtocolMessage

abstract class Protocol(
                         private val vertx: Vertx,
                         private val address: String,
                         private val database: HashMapDatabase,
                         private val timeout: Long = 5000L,
                         private val network: mutable.Map[String, NetworkState] = new mutable.HashMap[String, NetworkState]()
                       ) {
  private val COMMIT_PROTOCOL_ADDRESS = "commit-protocol"
  private val deliveryOptions: DeliveryOptions = DeliveryOptions().setSendTimeout(timeout)
  private val eventBus: EventBus = vertx.eventBus()

  network.put(address, READY)

  def listen(): Unit = {
    eventBus.consumer(COMMIT_PROTOCOL_ADDRESS, handler = (message: Message[Buffer]) => {
      onMessageReceived(message, message.body().toJsonObject.mapTo(classOf[ProtocolMessage]))
    })

    // perform a heart beat every second
    vertx.setPeriodic(1000L, _ => {
      sendToCohortExpectingReply(RequestNetwork(address, network(address)), (response: AsyncResult[Message[Buffer]]) => {
        if (response.succeeded()) {
          response.result().body().toJsonObject.mapTo(classOf[RespondNetwork]) match {
            case RespondNetwork(sender, state, _) => network.put(sender, state)
          }
        }
      })
    })
  }

  def performTransaction(transaction: Transaction): Unit = {
    transaction match {
      case StoreDataTransaction(_, key, data, _) => database.store(key, data)
      case RemoveDataTransaction(_, key, _) => database.remove(key)
    }
  }


  def sendToCohortExpectingReply[T](messageToSend: ProtocolMessage, handler: Handler[AsyncResult[Message[Buffer]]]): Unit = {
    eventBus.send(COMMIT_PROTOCOL_ADDRESS, Json.encodeToBuffer(messageToSend), deliveryOptions, handler)
  }

  def sendToCohort(messageToSend: ProtocolMessage): Unit = {
    eventBus.send(COMMIT_PROTOCOL_ADDRESS, Json.encodeToBuffer(messageToSend))
  }

  def requestTransaction(transaction: Transaction)

  def handleProtocolMessage(message: Message[Buffer], protocolMessage: ProtocolMessage)

  def onMessageReceived(message: Message[Buffer], protocolMessage: ProtocolMessage): Unit = {
    (protocolMessage, message.replyAddress()) match {
      case (RequestNetwork(sender, state, _), Some(_)) =>
        message.reply(Json.encodeToBuffer(RespondNetwork(address, network(address))))
        network.put(sender, state)
      case _ => handleProtocolMessage(message, protocolMessage)
    }
  }
}