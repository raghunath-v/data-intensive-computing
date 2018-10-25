package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
    

    //val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sparkConf = new SparkConf().setAppName("DirectKafkaAvg").setMaster("local[2]").set("spark.executor.memory","1g");
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(".")
    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
            "metadata.broker.list" -> "localhost:9092",
            "zookeeper.connect" -> "localhost:2181",
            "group.id" -> "kafka-spark-streaming",
            "zookeeper.connection.timeout.ms" -> "1000")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("avg"))
    
    val values = messages.map( _._2)
    val pairs = values.flatMap{ _.split(" ") }.map(x => (x, 1D)).reduceByKey(_ + _)
    //val pairs =values.flatMap{ _.split(" ") }.map(i => { var s=i.split(",") ( s(0), s(1).toInt) })

    // measure the average value for each key in a stateful manner
    /*def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	val curr_avg= state.get()
        val new_avg = (curr_avg +value.get)/2
                            
        val output = (key, new_avg)
        state.update(new_avg)
        //Some(output)
        return output
    }*/
   def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
      if (state.exists() && value.isDefined) {
        val es = state.get()        
        val nstate = (es + value.get) / 2 
        state.update(nstate)
        return (key, nstate)
      } else if (value.isDefined) {
        val in = value.get
        state.update(in)
        return (key, in)
      } else {
        return ("",0.0)
      }
    } 
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
    session.close()
  }
}
