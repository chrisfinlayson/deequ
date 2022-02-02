/**
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not
 * use this file except in compliance with the License. A copy of the License
 * is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.deequ.schema

import com.snowflake.snowpark.Column.expr
import com.snowflake.snowpark.functions.{col, length, not}
import com.snowflake.snowpark.types.{DecimalType, IntegerType, TimestampType}
import com.snowflake.snowpark.{Column, DataFrame}
//import org.apache.spark.storage.StorageLevel


sealed trait ColumnDefinition {
  def name: String
  def is_nullable: Boolean

  def castExpression(): Column = { col(name) }
}

private[this] case class StringColumnDefinition(
    name: String,
    is_nullable: Boolean = true,
    minLength: Option[Int] = None,
    maxLength: Option[Int] = None,
    matches: Option[String] = None)
  extends ColumnDefinition

private[this] case class IntColumnDefinition(
    name: String,
    is_nullable: Boolean = true,
    minValue: Option[Int] = None,
    maxValue: Option[Int] = None)
  extends ColumnDefinition {

  override def castExpression(): Column = { col(name).cast(IntegerType).as(name) }
}

private[this] case class DecimalColumnDefinition(
    name: String,
    precision: Int,
    scale: Int,
    is_nullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = { col(name).cast(DecimalType(precision, scale)).as(name) }
}

private[this] case class TimestampColumnDefinition(
    name: String,
    mask: String,
    is_nullable: Boolean = true)
  extends ColumnDefinition {

  override def castExpression(): Column = {
    unix_timestamp(col(name), mask).cast(TimestampType).as(name)
  }
}


/** A simple schema definition for relational data in Andes */
case class RowLevelSchema(columnDefinitions: Seq[ColumnDefinition] = Seq.empty) {

  /**
    * Declare a textual column
    *
    * @param name column name
    * @param is_nullable are NULL values permitted?
    * @param minLength  minimum length of values
    * @param maxLength  maximum length of values
    * @param matches regular expression which the column value must match
    * @return
    */
  def withStringColumn(
      name: String,
      is_nullable: Boolean = true,
      minLength: Option[Int] = None,
      maxLength: Option[Int] = None,
      matches: Option[String] = None)
    : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ StringColumnDefinition(name, is_nullable, minLength,
      maxLength, matches))
  }

  /**
    * Declare an integer column
    *
    * @param name column name
    * @param is_nullable are NULL values permitted?
    * @param minValue minimum value
    * @param maxValue maximum value
    * @return
    */
  def withIntColumn(
      name: String,
      is_nullable: Boolean = true,
      minValue: Option[Int] = None,
      maxValue: Option[Int] = None)
    : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ IntColumnDefinition(name, is_nullable, minValue, maxValue))
  }

  /**
    * Declare a decimal column
    *
    * @param name column name
    * @param precision  precision of values
    * @param scale  scale of values
    * @param is_nullable are NULL values permitted?
    * @return
    */
  def withDecimalColumn(
      name: String,
      precision: Int,
      scale: Int,
      is_nullable: Boolean = true)
    : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ DecimalColumnDefinition(name, precision, scale, is_nullable))
  }

  /**
    * Declare a timestamp column
    *
    * @param name column name
    * @param mask pattern for the timestamp
    * @param is_nullable are NULL values permitted?
    * @return
    */
  def withTimestampColumn(
      name: String,
      mask: String,
      is_nullable: Boolean = true)
    : RowLevelSchema = {

    RowLevelSchema(columnDefinitions :+ TimestampColumnDefinition(name, mask, is_nullable))
  }
}

/**
  * Result of enforcing a schema on textual data
  *
  * @param validRows  data frame holding the (casted) rows which conformed to the schema
  * @param numValidRows number of rows which conformed to the schema
  * @param invalidRows data frame holding the rows which did not conform to the schema
  * @param numInvalidRows number of rows which did not conform to the schema
  */
case class RowLevelSchemaValidationResult(
  validRows: DataFrame,
  numValidRows: Long,
  invalidRows: DataFrame,
  numInvalidRows: Long
)

/** Enforce a schema on textual data */
object RowLevelSchemaValidator {

  private[this] val MATCHES_COLUMN = "__deequ__matches__schema"

  /**
    * Enforces a schema on textual data, filters out non-conforming columns and casts the result
    * to the requested types
    *
    * @param data a data frame holding the data to validate in string-typed columns
    * @param schema the schema to enforce
    * @param storageLevelForIntermediateResults the storage level for intermediate results
    *                                           (to control caching behavior)
    * @return results of schema enforcement
    */
  def validate(
      data: DataFrame,
      schema: RowLevelSchema
//      ,
//      storageLevelForIntermediateResults: StorageLevel = StorageLevel.MEMORY_AND_DISK
    ): RowLevelSchemaValidationResult = {

    val dataWithMatches = data
      .withColumn(MATCHES_COLUMN, toCNF(schema))

//    dataWithMatches.persist(storageLevelForIntermediateResults)

    val validRows = extractAndCastValidRows(dataWithMatches, schema)
    val numValidRows = validRows.count()

    val invalidRows = dataWithMatches
      .where(not(col(MATCHES_COLUMN)))
      .drop(MATCHES_COLUMN)

    val numInValidRows = invalidRows.count()

//    dataWithMatches.unpersist(false)

    RowLevelSchemaValidationResult(validRows, numValidRows, invalidRows, numInValidRows)
  }

  private[this] def extractAndCastValidRows(
      dataWithMatches: DataFrame,
      schema: RowLevelSchema)
    : DataFrame = {

    val castExpressions = schema.columnDefinitions
      .map { colDef => colDef.name -> colDef.castExpression() }
      .toMap

    val projection = dataWithMatches.schema
      .map { _.name }
      .filter { _ != MATCHES_COLUMN }
      .map { name => castExpressions.getOrElse(name, col(name)) }

    dataWithMatches.select(projection: _*).where(col(MATCHES_COLUMN))
  }

  private[this] def toCNF(schema: RowLevelSchema): Column = {
    schema.columnDefinitions.foldLeft(expr(true.toString)) { case (cnf, columnDefinition) =>

      var nextCnf = cnf

      if (!columnDefinition.is_nullable) {
        nextCnf = nextCnf.and(col(columnDefinition.name).is_not_null)
      }

      val colis_null = col(columnDefinition.name).is_null

      columnDefinition match {

        case intDef: IntColumnDefinition =>

          val colAsInt = col(intDef.name).cast(IntegerType)

          /* null or successfully casted */
          nextCnf = nextCnf.and(colis_null.or(colAsInt.is_not_null))

          intDef.minValue.foreach { value =>
            nextCnf = nextCnf.and(colis_null.is_null.or(colAsInt.geq(value)))
          }

          intDef.maxValue.foreach { value =>
            nextCnf = nextCnf.and(colis_null.or(colAsInt.leq(value)))
          }

        case decDef: DecimalColumnDefinition =>

          val decType = DataTypes.createDecimalType(decDef.precision, decDef.scale)
          nextCnf = nextCnf.and(colis_null.or(col(decDef.name).cast(decType).is_not_null))

        case strDef: StringColumnDefinition =>

          strDef.minLength.foreach { value =>
            nextCnf = nextCnf.and(colis_null.or(length(col(strDef.name)).geq(value)))
          }

          strDef.maxLength.foreach { value =>
            nextCnf = nextCnf.and(colis_null.or(length(col(strDef.name)).leq(value)))
          }

          strDef.matches.foreach { regex =>
            nextCnf = nextCnf
              .and(colis_null.or(regexp_extract(col(strDef.name), regex, 0).notEqual("")))
          }

        case tsDef: TimestampColumnDefinition =>
          /* null or successfully casted */
          nextCnf = nextCnf.and(colis_null.or(unix_timestamp(col(tsDef.name), tsDef.mask)
            .cast(TimestampType).is_not_null))
      }

      nextCnf
    }
  }
}
