package numa
/**
  * @author ${Edurne y Ernesto}
  */
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object NumaKafka {

  
  def main(args : Array[String]) {
    implicit val spark: SparkSession = SparkSession.builder().master("local").appName("numa-kafka").getOrCreate()
    implicit val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val conf = ConfigFactory.load()
    val kafkaServer = conf.getString("kafka.kafka-server")
    val groupId = conf.getString("kafka.group-id")
    val topic = List(conf.getString("kafka.topic"))
    val autoOffset = conf.getString("kafka.auto-offset")
    val autoCommit = conf.getString("kafka.auto-commit")



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoOffset,
      "enable.auto.commit" -> autoCommit
    )
    val topics = topic
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, Subscribe[String, String](topics, kafkaParams)
    )
    messages.map(record => (record.key, record.value))

    import spark.implicits._

    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val dfWithOriginalMessages = rdd.map(_.value()).toDF
      dfWithOriginalMessages.show()
    }



    /*



    val messages = KafkaStreamReader.readStream(kafkaServers, inputTopic, groupId, securedKafka)
    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val dfWithOriginalMessages = rdd.map(_.value()).toDF(originalTextColumn)

  }*/

  }
}
