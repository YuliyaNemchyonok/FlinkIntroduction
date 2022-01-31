package ru.beeline.bigdata.flink_introduction

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import ru.beeline.bigdata.flink_introduction.entities._
import io.circe.parser.decode

import java.time.Duration

object Green {

  def apply()(implicit conf: ServiceConf,
              env: StreamExecutionEnvironment): DataStream[GreenEntity] = {

    val kafkaGreenConsumer: KafkaSource[String] = KafkaSource.builder[String]()
      .setTopics(conf.kafkaConf.greenTopicName)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .setProperties(conf.kafkaConf.kafkaConsumerParams)
      .build()

    val greenWatermark = WatermarkStrategy
      .forBoundedOutOfOrderness[String](Duration.ofSeconds(conf.kafkaConf.maxOutOfOrdernessSec))
      .withIdleness(Duration.ofSeconds(conf.kafkaConf.maxOutOfOrdernessSec))

    val pureDataDs = env.fromSource(
      kafkaGreenConsumer,
      greenWatermark,
      "Green Kafka Source")
      .setParallelism(conf.kafkaConf.greenNumPartitons)

    pureDataDs
      .map(decode[GreenRawEntity](_))
      .filter(_.isRight)
      .map(_.right.get)
      .map(raw => GreenEntity(
        raw.id,
        raw.timestamp,
        raw.session
      ))
  }


}
