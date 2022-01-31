package ru.beeline.bigdata.flink_introduction.entities

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

case class GreenRawEntity(
                           id: Long,
                           timestamp: Long,
                           session: String,
                           reserved1: Option[String],
                           reserved2: Option[String],
                           reserved3: Option[String]
                         )

object GreenRawEntity {
  implicit val decoder: Decoder[GreenRawEntity] = deriveDecoder
}