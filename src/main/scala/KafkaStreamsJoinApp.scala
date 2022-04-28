import org.apache.kafka.streams.StreamsConfig

import java.util.Properties

object KafkaStreamsJoinApp extends App {

  import org.apache.kafka.streams.KafkaStreams
  import org.apache.kafka.streams.scala._
  import org.apache.kafka.streams.scala.kstream._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._
  import org.apache.kafka.common.serialization.Serdes

  val builder = new StreamsBuilder()
  val amounts = builder.stream[String, String]("amounts")
  val rates = builder.table[String, String]("rates")
  val output = amounts
    .join(rates)((amount, rate) => s"Amount is ${amount.toDouble * rate.toDouble}")
  output.to("result")

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kantor-pipe")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100)
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

  val stream = new KafkaStreams(builder.build(), props)
  stream.start()
}