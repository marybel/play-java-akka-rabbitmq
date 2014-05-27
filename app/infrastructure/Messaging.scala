package infrastructure

import com.typesafe.config.ConfigFactory
//import akka.util.duration._
import scala.concurrent.duration._
import play.libs.Akka
import akka.actor.Props
import play.api.Logger
import com.rabbitmq.client.{Connection, ConnectionFactory, QueueingConsumer}
import com.rabbitmq.client.Channel
import akka.actor.Actor
import scala.concurrent.ExecutionContext.Implicits.global

object Config {
  val RABBITMQ_HOST = ConfigFactory.load().getString("rabbitmq.host")
  val RABBITMQ_QUEUE = ConfigFactory.load().getString("rabbitmq.queue")
  val RABBITMQ_EXCHANGEE = ConfigFactory.load().getString("rabbitmq.exchange")
}

object RabbitMQConnection {

  private val connection: Connection = null

  // Return a connection if one doesn't exist. Else create a new one
  def getConnection(): Connection = {
    connection match {
      case null => {
        val factory = new ConnectionFactory()
        factory.setHost(Config.RABBITMQ_HOST)
        factory.newConnection()
      }
      case _ => connection
    }
  }
}

object Sender {

  def startSending = {
    // create the connection
    val connection = RabbitMQConnection.getConnection()
    // create a new sending channel on which we declare the exchange
    val sendingChannel2 = connection.createChannel();
    sendingChannel2.exchangeDeclare(Config.RABBITMQ_EXCHANGEE, "fanout");

    // define the two callbacks for our listeners
    val callback3 = (x: String) => Logger.info("Recieved on exchange callback 3: " + x);
    val callback4 = (x: String) => Logger.info("Recieved on exchange callback 4: " + x);

    // create a channel for the listener and setup the first listener
    val listenChannel1 = connection.createChannel();
    setupListener(listenChannel1,listenChannel1.queueDeclare().getQueue(),
      Config.RABBITMQ_EXCHANGEE, callback3);

    // create another channel for a listener and setup the second listener
    val listenChannel2 = connection.createChannel();
    setupListener(listenChannel2,listenChannel2.queueDeclare().getQueue(),
      Config.RABBITMQ_EXCHANGEE, callback4);

    // create an actor that is invoked every two seconds after a delay of
    // two seconds with the message "msg"
    Akka.system.scheduler.schedule(2 seconds, 1 seconds, Akka.system.actorOf(Props(
      new PublishingActor(channel = sendingChannel2
        , exchange = Config.RABBITMQ_EXCHANGEE))),
      "MSG to Exchange");
  }

  private def setupListener(channel: Channel, queueName : String, exchange: String, f: (String) => Any) {
    channel.queueBind(queueName, exchange, "");

    Akka.system.scheduler.scheduleOnce(2 seconds,
      Akka.system.actorOf(Props(new ListeningActor(channel, queueName, f))), "");
  }
}

class PublishingActor(channel: Channel, exchange: String) extends Actor {

  /**
   * When we receive a message we sent it using the configured channel
   */
  def receive = {
    case some: String => {
      val msg = (some + " : " + System.currentTimeMillis());
      channel.basicPublish(exchange, "", null, msg.getBytes());
      Logger.info(msg);
    }
    case _ => {}
  }
}


class ListeningActor(channel: Channel, queue: String, f: (String) => Any) extends Actor {

  // called on the initial run
  def receive = {
    case _ => startReceving
  }

  def startReceving = {

    val consumer = new QueueingConsumer(channel)
    channel.basicConsume(queue, true, consumer)

    while (true) {
      // wait for the message
      val delivery = consumer.nextDelivery()
      val msg = new String(delivery.getBody())

      // send the message to the provided callback function and execute this in a subactor
      context.actorOf(Props(new Actor {
        def receive = {
          case some: String => f(some)
        }
      })) ! msg
    }
  }

}