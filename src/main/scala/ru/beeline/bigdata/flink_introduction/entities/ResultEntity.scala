package ru.beeline.bigdata.flink_introduction.entities

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class ResultEntity(
                         greenId: Long,
                         redId: Long,
                         session: String,
                         timestamp: Long
                       )

object ResultEntity {
  implicit val encoder: Encoder[ResultEntity] = deriveEncoder
}