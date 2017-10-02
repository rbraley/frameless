package frameless.functions

import frameless._
import org.apache.spark.sql.{functions => untyped}


trait NonAggregateFunctions {

  /** Non-Aggregate function: returns the absolute value of a numeric column
    *
    * apache/spark
    */
  def abs[A: CatalystNumeric, T](column:TypedColumn[T,A]):TypedColumn[T,A] = {
    implicit val c = column.uencoder
    new TypedColumn[T,A](untyped.abs(column.untyped))
  }

  /** Non-Aggregate function: returns the acos of a numeric column
    *
    * Spark will expect a Double value for this expression. See:
    *   [[https://github.com/apache/spark/blob/4a3c09601ba69f7d49d1946bb6f20f5cfe453031/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L67]]
    * apache/spark
    */
  def acos[A, T](column:TypedColumn[T,A])
    (implicit evCanBeDouble: CatalystCast[A, Double]):TypedColumn[T,A] = {
    implicit val c = column.uencoder
    new TypedColumn[T,A](untyped.acos(column.cast[Double].untyped))
  }


  /** Non-Aggregate function: returns the date the defined number of months later
    *
    * apache/spark
    */
  def add_months[A : CatalystDateTime, T](column:TypedColumn[T,A], numMonths:Int):TypedColumn[T,SQLDate] ={
    implicit val c = column.uencoder
    new TypedColumn[T,SQLDate](untyped.add_months(column.untyped, numMonths))
  }



}
