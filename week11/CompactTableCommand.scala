package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class CompactTableCommand(table: TableIdentifier,
  filesNum: Option[String],
  partitionSpec: Option[String])
  extends LeafRunnableCommand {

  private val defaultSize = 128 * 1024 * 1024

  override def output: Seq[Attribute] = Seq(
    AttributeReference("COMPACT_TABLE", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.setCurrentDatabase(table.database.getOrElse("default"))


    val tempTableName = "`" + table.identifier + "_" + System.currentTimeMillis() + "`"

    val originDataFrame = sparkSession.table(table.identifier)
    val partitions = filesNum match {
      case Some(files) => files.toInt
      case None => (sparkSession.sessionState
        .executePlan(originDataFrame.queryExecution.logical)
        .optimizedPlan.stats.sizeInBytes / defaultSize).toInt + 1
    }
    // scalastyle:off println
    println(partitions, tempTableName)

    if (partitionSpec.nonEmpty) {
      // https://stackoverflow.com/questions/38487667/
      // overwrite-specific-partitions-in-spark-dataframe-write-method
      sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

      val conditionExpr = partitionSpec.get.trim.stripPrefix("partition(").dropRight(1)
        .replace(",", "AND")
      // scalastyle:off println
      println(conditionExpr)

      originDataFrame.where(conditionExpr).repartition(partitions)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tempTableName)

      sparkSession.table(tempTableName).write
        .mode(SaveMode.Overwrite)
        .insertInto(table.identifier)
    } else {
      //
      originDataFrame.repartition(partitions)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tempTableName)

      sparkSession.table(tempTableName)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(table.identifier)
    }

    // sparkSession.sql(s"DROP TABLE ${tempTableName}")

    Seq(Row(s"compact table ${table.identifier} finished."))
  }
}