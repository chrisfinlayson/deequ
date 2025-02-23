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

package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isNumeric}
import com.snowflake.snowpark.{Column, Row}
import com.snowflake.snowpark.functions.max
import com.snowflake.snowpark.types.{DoubleType, StructType}
import Analyzers._

case class MaxState(maxValue: Double) extends DoubleValuedState[MaxState] {

  override def sum(other: MaxState): MaxState = {
    MaxState(math.max(maxValue, other.maxValue))
  }

  override def metricValue(): Double = {
    maxValue
  }
}

case class Maximum(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[MaxState]("Maximum", column)
  with FilterableAnalyzer {

  override def aggregationFunctions(): Seq[Column] = {
    max(conditionalSelection(column, where)).cast(DoubleType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MaxState] = {

    ifNoNullsIn(result, offset) { _ =>
      MaxState(result.getDouble(offset))
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isNumeric(column) :: Nil
  }

  override def filterCondition: Option[String] = where
}
