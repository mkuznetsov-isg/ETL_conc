package Enrichment

import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import org.quartz.CronExpression

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Date
import com.cronutils.parser.CronParser

object Test2 extends App {

//  val cronExp = "0 0 0 * * ?"
//
//  val today = new Date()
//
//  val cronTrigger = new CronExpression("0 0 0 * * ?")

//  cronTrigger.

//  val next = cronTrigger.getTimeBefore(today)
//  println(next)
//
//  val prev = cronTrigger.getTimeAfter(today)
//  println(prev)

//  1608017702.553158
//  1556175797428L

  val epoch = BigDecimal(1608017702.553158)
  val arr = epoch /% BigDecimal(1)

  val seconds = arr._1.toLongExact
  println(seconds)

  val nanos = arr._2.toString.split("\\.")(1).toLong * 1000
  println(nanos)

  val zdt = Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.of("Europe/Moscow"))
  println(zdt)

  import com.cronutils.model.CronType.QUARTZ

  val zoneId = ZoneId.of("UTC+1")
  val beginOfMonth = ZonedDateTime.of(2020, 12, 5, 5, 59, 59, 123, zoneId)
  println(beginOfMonth)

  val today2 = ZonedDateTime.now
  println(today2)

  val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(QUARTZ)
  val parser = new CronParser(cronDefinition)

  val cron = ExecutionTime.forCron(parser.parse("0 0 0 1/10 * ?"))
  val prev2 = cron.lastExecution(beginOfMonth)
  println(prev2)

  val next2 = cron.nextExecution(beginOfMonth)
  println(next2)

//  val yearPart = s"${prev2.get.getYear}-${next2.get.getYear}"
//  println(yearPart)
//
//  val monthPart = s"${prev2.get.getMonthValue}-${next2.get.getMonthValue}"
//  println(monthPart)
//
//  val dayPart = s"${prev2.get.getDayOfMonth}-${next2.get.getDayOfMonth}"
//  println(dayPart)
//
//  val rangePart = s"${prev2.get.toEpochSecond}-${next2.get.toEpochSecond}"
//  println(rangePart)

}
