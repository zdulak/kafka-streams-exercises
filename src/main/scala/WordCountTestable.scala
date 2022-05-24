import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.serialization.Serdes._

object WordCountTestable {
  def main(args: Array[String]): Unit = {
    import org.apache.kafka.streams.StreamsConfig
    import java.util.Properties

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-pipe")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    //  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    //  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

    val stream = new KafkaStreams(getTopology, props)
    stream.start()
  }

  def getTopology: Topology = {
    val builder = new StreamsBuilder()
    val textLines = builder.stream[String, String]("text-topic")
    val wordCounts = textLines
      .flatMapValues(textLine => textLine.split("\\W+"))
      .groupBy((key, word) => word)
      .count()
      .mapValues(_.toString)
    wordCounts.toStream.to("count-topic")
    builder.build()
  }
}
