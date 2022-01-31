package ru.beeline.bigdata.flink_introduction

case class KafkaConf(
                      kafkaConsumerParams: Map[String, String],
                      kafkaProducerParams: Map[String, String],
                      greenTopicName: String,
                      greenNumPartitons: Int,
                      redTopicName: String,
                      redNumPartitons: Int,
                      maxOutOfOrdernessSec: Int,
                      resultTopicName: String,
                      sideResultTopicName: String
                    )

case class ServiceConf(
                        version: String,
                        kafkaConf: KafkaConf,
                        timeToLiveStateMin: Int = 1,
                        timerLengthSec: Long = 30,
                        resultSinkPath: String
                      )

