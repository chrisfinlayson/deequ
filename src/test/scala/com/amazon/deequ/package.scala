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

package com.amazon

import com.snowflake.snowpark.{DataFrame,Row,Session}
import com.snowflake.snowpark.types.{StructField, StructType}
import com.snowflake.snowpark.types.{ DataType => SparkDT }

package object deequ {
  def dataFrameWithColumn(
      name: String,
      columnType: SparkDT,
      Session: Session,
      values: Row*)
    : DataFrame = {

    import scala.collection.JavaConverters._
    val struct = StructType(StructField(name, columnType) :: Nil)
    Session.createDataFrame(values.asJava, struct).toDF(name)
  }
}
