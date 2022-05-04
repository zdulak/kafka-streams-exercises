object WordCountApp extends App {

  import org.apache.kafka.streams.StreamsConfig

  import java.util.Properties

  val props = new Properties()
  val appId = this.getClass.getSimpleName.replace("$", "")
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)

  import org.apache.kafka.streams.KafkaStreams
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val builder = new StreamsBuilder()
  builder.stream[String, String]("text-topic")
    .flatMapValues(textLine => textLine.split("\\W+"))
    .groupBy((_, word) => word)
    .count()
    .mapValues(_.toString)
    .toStream
    .to("count-topic")
  private val topology = builder.build()

  val stream = new KafkaStreams(topology, props)
  stream.start()
}
