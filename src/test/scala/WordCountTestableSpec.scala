import org.apache.kafka.streams.state.ValueAndTimestamp
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util.Properties
import scala.jdk.CollectionConverters.MapHasAsScala

class WordCountTestableSpec extends AnyFlatSpec with should.Matchers {
  def getFixture = new {
    import org.apache.kafka.streams.StreamsConfig
    import org.apache.kafka.streams.TopologyTestDriver
    import org.apache.kafka.common.serialization.Serdes
    val props = new Properties()
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    val stringSerde =  new Serdes.StringSerde()

    val testDriver = new TopologyTestDriver(WordCountTestable.getTopology, props)
    val inputTopic = testDriver
      .createInputTopic("text-topic", stringSerde.serializer(), stringSerde.serializer())
    val outputTopic =  testDriver
      .createOutputTopic("count-topic", stringSerde.deserializer(), stringSerde.deserializer())
  }

  behavior of "WordCountTestable"

  it should "return all intermediate states for the value of a record: ala ala" in {
//    Arrange
    val f = getFixture
//    Act
    f.inputTopic.pipeInput("ala ala")
//    Assert
//    Read about store in kafka streams
    f.outputTopic.readKeyValue() shouldBe KeyValue.pair("ala", "1")
    f.outputTopic.readKeyValue() shouldBe KeyValue.pair("ala", "2")
  }

  it should "return 2 for the value of a record: ala ala" in {
    //    Arrange
    val f = getFixture
    //    Act
    f.inputTopic.pipeInput("ala ala")
    //    Assert

//    val store = f
//      .testDriver
//      .getAllStateStores
//      .asScala
//      .head._2
//      .asInstanceOf[MeteredKeyValueStore[String, ValueAndTimestamp[Long]]]
//    f.testDriver.getAllStateStores.asScala.foreach(println)
//    store.get("ala").value() shouldBe 2
    f.outputTopic.readKeyValuesToMap().get("ala") shouldBe "2"
  }

}
