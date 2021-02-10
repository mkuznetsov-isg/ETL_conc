package Sheduler

import Config.ConfigProc
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

import java.util.Date
import org.quartz.CronScheduleBuilder.cronSchedule
import org.quartz.JobBuilder.newJob
import org.quartz.TriggerBuilder.newTrigger

import java.io.File
import scala.util.Try

object Test1 extends App {

  val conf: ConfigProc = ConfigProc("D:\\scala_work\\ETL_conc\\src\\main\\resources\\application.conf")
  val trSpan: Try[String] = conf.transformerSpan

  // TODO disable SLF4J logger

  if (trSpan.isSuccess) {
    val crExpIsValid: Boolean = CronExpression.isValidExpression(trSpan.get)
    println(crExpIsValid)

    if (crExpIsValid) {
      val schedulerFactory: StdSchedulerFactory = new StdSchedulerFactory
      val scheduler: Scheduler = schedulerFactory.getScheduler

      val job: JobDetail = newJob(classOf[ExampleJob]).withIdentity("ExJob1", "ExGroupJob1").build

      val str: Try[String] = conf.storage
      if (str.isSuccess) {
        val storage: String = str.get
        val pathToStorage: File = new File(storage)

        if (pathToStorage.exists) job.getJobDataMap.put("storage", storage)
      }

      val trigger: CronTrigger =
        newTrigger
          .withIdentity("ExTrigger1", "ExGroupTrigger1")
          .withSchedule(cronSchedule(trSpan.get))
          .build

      val date: Date = scheduler.scheduleJob(job, trigger)

      println(s"${job.getKey} has been scheduled to run at: $date and is repeated based on the cron expression: ${trigger.getCronExpression}")

      scheduler.start()


      val x: Int = 4
      println(s"Doing something after scheduler has been started ... $x")

    }
  }

}
