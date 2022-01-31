package ru.beeline.bigdata.flink_introduction

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import ru.beeline.bigdata.flink_introduction.entities.{GreenEntity, RedEntity, ResultEntity}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class MatchingFunction(conf: ServiceConf, outputTag: OutputTag[String]) extends KeyedCoProcessFunction[Long, RedEntity, GreenEntity, ResultEntity] {

  val ttlConfig: StateTtlConfig = StateTtlConfig
    .newBuilder(Time.minutes(conf.timeToLiveStateMin))
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .cleanupFullSnapshot()
    .build

  val redStateDescriptor: ListStateDescriptor[RedEntity] = new ListStateDescriptor[RedEntity]("red list", classOf[RedEntity])
  redStateDescriptor.enableTimeToLive(ttlConfig)

  lazy val redState: ListState[RedEntity] = getRuntimeContext.getListState(redStateDescriptor)


  val greenStateDescriptor: ListStateDescriptor[GreenEntity] = new ListStateDescriptor[GreenEntity]("green list", classOf[GreenEntity])
  greenStateDescriptor.enableTimeToLive(ttlConfig)

  lazy val greenState: ListState[GreenEntity] = getRuntimeContext.getListState(greenStateDescriptor)


  override def processElement1(red: RedEntity,
                               context: KeyedCoProcessFunction[Long, RedEntity, GreenEntity, ResultEntity]#Context,
                               out: Collector[ResultEntity]): Unit = {

    if (greenState.get().asScala.nonEmpty && redState.get().asScala.isEmpty) {
      val timer: Long = (context.timerService().currentProcessingTime() / 1000) + conf.timerLengthSec
      context.timerService().registerEventTimeTimer(timer)
    }

    redState.add(red)
  }

  override def processElement2(green: GreenEntity,
                               context: KeyedCoProcessFunction[Long, RedEntity, GreenEntity, ResultEntity]#Context,
                               out: Collector[ResultEntity]): Unit = {

    if (redState.get().asScala.nonEmpty && greenState.get().asScala.isEmpty) {
      val timer: Long = (context.timerService().currentProcessingTime() / 1000) + conf.timerLengthSec
      context.timerService().registerEventTimeTimer(timer)
    }

    greenState.add(green)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedCoProcessFunction[Long, RedEntity, GreenEntity, ResultEntity]#OnTimerContext,
                       out: Collector[ResultEntity]): Unit = {

    val redList: Seq[RedEntity] = redState.get.asScala.toList.distinct
    val greenList = greenState.get.asScala.toList.distinct

    val combined = for {
      a <- redList
      b <- greenList
    } yield (a, b)

    combined.foldLeft(None: Option[(RedEntity, GreenEntity)]) {
      case (None, x) => Some(x)
      case (Some((g0, r0)), next@(g1, r1)) if (g0.timestamp - r0.timestamp).abs > (g1.timestamp - r1.timestamp).abs => Some(next)
      case (acc, _) => acc
    }
      .foreach { case (red: RedEntity, green: GreenEntity) =>
        out.collect(
          ResultEntity(
            green.id,
            red.id,
            red.session,
            Math.min(red.timestamp, green.timestamp)
          ))

        combined
          .filter(tuple => tuple._1 != red && tuple._2 != green)
          .map { case (red: RedEntity, green: GreenEntity) => (red, green, (red.timestamp - green.timestamp).abs) }
          .foreach { case (wrongRed: RedEntity, wrongGreen: GreenEntity, diffTs: Long) =>
            ctx.output(
              outputTag,
              s"not-matched: green($wrongGreen), red($wrongRed), diff=$diffTs"
            )
          }
      }

    redState.clear()
    greenState.clear()
  }
}
