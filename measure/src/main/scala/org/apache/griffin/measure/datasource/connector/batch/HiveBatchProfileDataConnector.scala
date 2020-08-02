/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure.datasource.connector.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.context.TimeRange
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * batch data connector for hive table
 */
/**
 * This connector will will aggregate data from the data source based on the mentioned metrics in connector config file
 * instead of pulling all data and processes locally.
 */
case class HiveBatchProfileDataConnector(
    @transient sparkSession: SparkSession,
    dcParam: DataConnectorParam,
    timestampStorage: TimestampStorage)
    extends BatchDataConnector {

  val config: Map[String, Any] = dcParam.getConfig

  val Database = "database"
  val TableName = "table.name"
  val Where = "where"
  val Metrics = "metrics"

  val database: String = config.getString(Database, "default")
  val tableName: String = config.getString(TableName, "")
  val whereString: String = config.getString(Where, "")
  val metrics: Seq[String] = config.getStringArr(Metrics, defValue = Seq("total_count"))
  val concreteTableName = s"$database.$tableName"

  def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val dfOpt = {
      val dtSql = dataSql()
      info(dtSql)
      val df = sparkSession.sql(dtSql)
      val dfOpt = Some(df)
      val preDfOpt = preProcess(dfOpt, ms)
      preDfOpt
    }
    val tmsts = readTmst(ms)
    (dfOpt, TimeRange(ms, tmsts))
  }

  private def dataSql(): String = {
    var selectString: String = ""
    if (metrics.indexOf("total_count") > -1) {
      selectString = " count(*) as total_count "
    }

    // ADD OTHER METRICS HERE CONCAT WITH selectString

    var tableClause = s"SELECT $selectString FROM $concreteTableName"
    if (whereString.length > 0) {
      tableClause = s"$tableClause WHERE $whereString"
    }
    tableClause
  }

}
