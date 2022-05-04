object KafkaStreamsJoinApp extends App {

  import org.apache.kafka.common.serialization.Serdes
  import org.apache.kafka.streams.KafkaStreams
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val builder = new StreamsBuilder()
  val rates = builder.table[String, String]("rates")
  val output = builder
    .stream[String, String]("amounts")
    .join(rates)((amount, rate) => s"Amount is ${amount.toDouble * rate.toDouble}")
    .to("result")
  private val topology = builder.build()

  import java.util.Properties
  val props = new Properties()
  val appId = this.getClass.getSimpleName.replace("$", "")
  import org.apache.kafka.streams.StreamsConfig
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100)
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

  val stream = new KafkaStreams(topology, props)
  stream.start()
}