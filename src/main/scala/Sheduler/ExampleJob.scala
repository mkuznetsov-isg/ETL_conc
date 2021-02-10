package Sheduler

import org.quartz.{Job, JobDataMap, JobExecutionContext}

import java.time.LocalDateTime

class ExampleJob extends Job {
  def execute(context: JobExecutionContext): Unit = {
    println(s"Job executed at: ${LocalDateTime.now.toString}")

    val data: JobDataMap = context.getJobDetail.getJobDataMap
    val storage: String = data.get("storage").asInstanceOf[String]
    println(storage)

    println(s"Next job will be executed at: ${context.getFireTime.toString}")
  }
}
