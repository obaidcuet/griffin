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

import scala.util._

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.context.TimeRange
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * A batch data connector for JDBC based source which allows support for various
 * JDBC based data sources like Oracle. Postgres etc.
 *
 * Supported Configurations:
 *  - database : [[String]] specifying the database name.
 *  - tablename : [[String]] specifying the table name to be read
 *  - select : [[String]] specifying the columns or aggregation as we do in sql after "select" statement
 *  - url : [[String]] specifying the URL to connect to database
 *  - user : [[String]] specifying the user for connection to database
 *  - password: [[String]] specifying the password for connection to database
 *  - driver : [[String]] specifying the driver for JDBC connection to database
 *  - where : [[String]] specifying the condition for reading data from table
 *  - group.by : [[String]] specifying the group by part on the sql query in case doing aggregation in "select"
 *
 * Some defaults assumed by this connector (if not set) are as follows:
 *  - `database` is default,
 *  - `driver` is com.mysql.jdbc.Driver,
 *  - `where` is None
 */
/**
 * This connector will will aggregate data from the data source based on the mentioned metrics in connector config file
 * instead of pulling all data and processes locally.
 */
case class JDBCBasedProfileDataConnector(
    @transient sparkSession: SparkSession,
    dcParam: DataConnectorParam,
    timestampStorage: TimestampStorage)
    extends BatchDataConnector {

  import JDBCBasedProfileDataConnector._
  val config: Map[String, Any] = dcParam.getConfig
  val database: String = config.getString(Database, DefaultDatabase)
  val tableName: String = config.getString(TableName, EmptyString)
  val fullTableName: String = s"$database.$tableName"
  val whereString: String = config.getString(Where, EmptyString)
  val metrics: Seq[String] = config.getStringArr(Metrics, defValue = Seq("total_count"))
  val url: String = config.getString(Url, EmptyString)
  val user: String = config.getString(User, EmptyString)
  val password: String = config.getString(Password, EmptyString)
  val driver: String = config.getString(Driver, DefaultDriver)

  require(url.nonEmpty, "JDBC connection: connection url is mandatory")
  require(user.nonEmpty, "JDBC connection: user name is mandatory")
  require(password.nonEmpty, "JDBC connection: password is mandatory")
  require(tableName.nonEmpty, "JDBC connection: table is mandatory")
  assert(isJDBCDriverLoaded(driver), s"JDBC driver $driver not present in classpath")

  override def data(ms: Long): (Option[DataFrame], TimeRange) = {
    val dfOpt = try {
      val dtSql = createSqlStmt()
      val prop = new java.util.Properties
      prop.setProperty("user", user)
      prop.setProperty("password", password)
      prop.setProperty("driver", driver)
      val dfOpt = Try(sparkSession.read.jdbc(url, s"($dtSql) as t", prop))

      dfOpt match {
        case Success(_) =>
        case Failure(exception) =>
          griffinLogger.error("Error occurred while reading data set.", exception)
      }

      val preDfOpt = preProcess(dfOpt.toOption, ms)
      preDfOpt
    } catch {
      case e: Throwable =>
        error(s"loading table $fullTableName fails: ${e.getMessage}", e)
        None
    }
    val tmsts = readTmst(ms)
    (dfOpt, TimeRange(ms, tmsts))
  }

  /**
   * @return Return SQL statement with where condition if provided
   */
  private def createSqlStmt(): String = {
    var selectString: String = ""
    if (metrics.indexOf("total_count") > -1) {
      selectString = " count(*) as total_count "
    }

    // ADD OTHER METRICS HERE CONCAT WITH selectString

    var tableClause = s"SELECT $selectString FROM $fullTableName"
    if (whereString.length > 0) {
      tableClause = s"$tableClause WHERE $whereString"
    }
    tableClause
  }
}

object JDBCBasedProfileDataConnector extends Loggable {
  private val Database: String = "database"
  private val TableName: String = "tablename"
  private val Where: String = "where"
  private val Metrics = "metrics"
  private val Url: String = "url"
  private val User: String = "user"
  private val Password: String = "password"
  private val Driver: String = "driver"

  private val DefaultDriver = "com.mysql.jdbc.Driver"
  private val DefaultDatabase = "default"
  private val EmptyString = ""

  /**
   * @param driver JDBC driver class name
   * @return True if JDBC driver present in classpath
   */
  private def isJDBCDriverLoaded(driver: String): Boolean = {
    try {
      Class.forName(driver, false, this.getClass.getClassLoader)
      true
    } catch {
      case e: ClassNotFoundException =>
        griffinLogger.error(s"JDBC driver $driver provided is not found in class path", e)
        false
    }
  }
}
