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
 * The difference between this and HiveBatchDataConnector is, this will aggregate data from the data source
 * instead of pulling all data and processes locally.
 */

case class HiveBatchDataConnectorExtension(
                                   @transient sparkSession: SparkSession,
                                   dcParam: DataConnectorParam,
                                   timestampStorage: TimestampStorage)
  extends BatchDataConnector {

  val config: Map[String, Any] = dcParam.getConfig

  val Database = "database"
  val TableName = "table.name"
  val SelectString = "select"
  val Where = "where"
  val GroupBy = "group.by"

  val database: String = config.getString(Database, "default")
  val tableName: String = config.getString(TableName, "")
  val selectString: String = config.getString(SelectString, "*")
  val whereString: String = config.getString(Where, "")
  val groupBy: String = config.getString(GroupBy, "")

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
    var tableClause = s"SELECT $selectString FROM $concreteTableName"
    if (whereString.length > 0) {
      tableClause = s"$tableClause WHERE $whereString"
    }
    if (groupBy.length > 0) {
      tableClause = s"$tableClause group by $groupBy"
    }
    tableClause
  }

}
