package ru.beeline.bigdata.flink_introduction

import pureconfig.generic.auto._
import io.circe.syntax.EncoderOps
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import pureconfig.ConfigSource
import ru.beeline.bigdata.flink_introduction.entities.ResultEntity


object App {

  def main(args: Array[String]): Unit = {

    implicit val conf: ServiceConf = ConfigSource.default.load[ServiceConf].right.get

    println(conf)

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableUnalignedCheckpoints()
    env.enableCheckpointing(300000)

    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      15, // max failures per unit
      Time.minutes(15), //time interval for measuring failure rate
      Time.seconds(10) // delay
    ))

    val greenDs = Green.apply

    val redDs = Red.apply

    val outputTag = OutputTag[String]("not-matched-events")

    val resultStream = redDs
      .keyBy(_.session)
      .connect(greenDs.keyBy(_.session))
      .process(new MatchingFunction(conf, outputTag))
      .name("Matching function")
      .setParallelism(10)

    val resultSink = KafkaSink.builder[String]()
      .setKafkaProducerConfig(conf.kafkaConf.kafkaConsumerParams)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(conf.kafkaConf.resultTopicName)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()

    resultStream.map(_.asJson.noSpaces).sinkTo(resultSink)

    val sideOutputStream: DataStream[String] = resultStream.getSideOutput(outputTag)

    val sideResultSink = KafkaSink.builder[String]()
      .setKafkaProducerConfig(conf.kafkaConf.kafkaConsumerParams)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder[String]()
        .setTopic(conf.kafkaConf.sideResultTopicName)
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()

    sideOutputStream.sinkTo(sideResultSink)

    val resultFileSink: StreamingFileSink[ResultEntity] = StreamingFileSink
      .forBulkFormat(new Path(conf.resultSinkPath), ParquetAvroWriters.forReflectRecord(classOf[ResultEntity]))
      .build()

    env.setParallelism(1)

    resultStream.addSink(resultFileSink)

    env.execute()


  }
}
