package ru.beeline.bigdata.flink_introduction

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import ru.beeline.bigdata.flink_introduction.entities.{RedEntity, RedRawEntity}
import purecsv.safe.CSVReader

import java.time.Duration

object Red {

  def apply()(implicit conf: ServiceConf,
              env: StreamExecutionEnvironment): DataStream[RedEntity] = {

    // this topic contains csv strings
    val kafkaRedConsumer = KafkaSource.builder[String]()
      .setTopics(conf.kafkaConf.redTopicName)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperties(conf.kafkaConf.kafkaConsumerParams)
      .build()

    val redWatermark = WatermarkStrategy
      .forBoundedOutOfOrderness[String](Duration.ofSeconds(conf.kafkaConf.maxOutOfOrdernessSec))
      /*      attempt to change timestamp of the event:
            .withTimestampAssigner(new SerializableTimestampAssigner[String] {
              override def extractTimestamp(record: String, recordTimestamp: Long): Long = {
                CSVReader[RedRawEntity].readCSVFromString(record).head match {
                  case Success(red) => red.timestamp
                  case Failure(exception) => 0L
                }
              }
            })*/
      .withIdleness(Duration.ofSeconds(conf.kafkaConf.maxOutOfOrdernessSec))


    val pureDataDs = env.fromSource(
      kafkaRedConsumer,
      redWatermark,
      "Red Kafka Consumer")
      .setParallelism(conf.kafkaConf.redNumPartitons)

    pureDataDs
      .rebalance
      .map(CSVReader[RedRawEntity].readCSVFromString(_).head)
      .filter(_.isSuccess)
      .map(_.get)
      .map(raw => RedEntity(
        raw.id,
        raw.timestamp,
        raw.session
      ))
  }
}
