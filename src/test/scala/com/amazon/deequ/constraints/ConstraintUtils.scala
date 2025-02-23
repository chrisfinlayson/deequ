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

package com.amazon.deequ.constraints

import com.snowflake.snowpark.DataFrame

object ConstraintUtils {

  def calculate(constraint: Constraint, df: DataFrame): ConstraintResult = {

    val analysisBasedConstraint = constraint match {
        case nc: ConstraintDecorator => nc.inner
        case c: Constraint => c
    }

    analysisBasedConstraint.asInstanceOf[AnalysisBasedConstraint[_, _, _]].calculateAndEvaluate(df)
  }
}
