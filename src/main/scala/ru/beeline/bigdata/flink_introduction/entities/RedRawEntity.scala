package ru.beeline.bigdata.flink_introduction.entities

case class RedRawEntity(
                         id: Long,
                         timestamp: Long,
                         session: String,
                         reserved1: Option[String],
                         reserved2: Option[String],
                         reserved3: Option[String]
                       )
