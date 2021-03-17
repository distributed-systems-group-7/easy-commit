package l.tudelft.distribted.ec.protocols

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.lang.scala.json.JsonObject
import io.vertx.scala.core.Vertx
import io.vertx.scala.core.eventbus.{DeliveryOptions, EventBus, Message}
import l.tudelft.distribted.ec.{HashMapDatabase, protocols}
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
  val READY: protocols.NetworkState.Value = Value
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(Array(
  new Type(value = classOf[RequestNetwork], name = "request.network"),
  new Type(value = classOf[TransactionPrepareRequest], name = "request.prepare"),
  new Type(value = classOf[TransactionReadyResponse], name = "request.prepare"),
  new Type(value = classOf[TransactionAbortResponse], name = "request.abort"),
  new Type(value = classOf[TransactionCommitRequest], name = "request.commit"),
))
trait ProtocolMessage {
  def sender: String

  def `type`: String
}


case class RequestNetwork(sender: String, state: NetworkState, `type`: String = "request.network") extends ProtocolMessage

case class TransactionPrepareRequest(sender: String, id: String, transaction: Transaction, `type`: String = "request.prepare") extends ProtocolMessage

case class TransactionReadyResponse(sender: String, id: String, `type`: String = "response.prepare") extends ProtocolMessage

case class TransactionAbortResponse(sender: String, id: String, `type`: String = "response.abort") extends ProtocolMessage

case class TransactionCommitRequest(sender: String, id: String, `type`: String = "request.commit") extends ProtocolMessage

abstract class Protocol(
                         private val vertx: Vertx,
                         private val address: String,
                         private val database: HashMapDatabase,
                       ) {
  private val network: NetworkingHandler = new NetworkingHandler(vertx, address)

  def listen(): Unit = {
    network.listen()
    vertx.eventBus().consumer(address, handler = (message: Message[Buffer]) => {
      handleProtocolMessage(message, message.body().toJsonObject.mapTo(classOf[ProtocolMessage]))
    })
  }

  def performTransaction(transaction: Transaction): Unit = {
    transaction match {
      case StoreDataTransaction(_, key, data, _) => database.store(key, data)
      case RemoveDataTransaction(_, key, _) => database.remove(key)
    }
  }


  def sendToCohortExpectingReply[T](messageToSend: ProtocolMessage, handler: Handler[AsyncResult[Message[Buffer]]]): Unit = {
    network.sendToCohortExpectingReply(messageToSend, handler)
  }

  def sendToCohort(messageToSend: ProtocolMessage): Unit = {
    network.sendToCohort(messageToSend)
  }

  def replyToMessage(message: Message[Buffer], messageToSend: ProtocolMessage): Unit = {
    message.reply(Json.encode(messageToSend))
  }

  def sendToAddress(address: String, messageToSend: ProtocolMessage): Unit = {
    eventBus.send(address, Json.encodeToBuffer(messageToSend))
  }

  abstract def requestTransaction(transaction: Transaction)

  abstract def handleProtocolMessage(message: Message[Buffer], protocolMessage: ProtocolMessage)

  def handleProtocolMessage(message: Message[Buffer], protocolMessage: ProtocolMessage)
}
