package frameless
package functions

import java.sql.Date

import frameless.functions.nonAggregate._
import org.scalacheck.Prop._
import org.apache.spark.sql.Encoder

class NonAggregateFunctionsTests extends TypedDatasetSuite {

  test("add_months"){
    val spark = session
    import spark.implicits._
    implicit val e = org.apache.spark.sql.Encoders.DATE


    //add_months returns java.sql.Date due to the upCasting in ScalaReflection.upCastToExpectedType
    //so we need appropriate converters from frameless internal date representation to java.sql.Date for proper testing.
    abstract class ToJSQLDate[A:CatalystDateTime]{
      def toJSQLDate(a:A):java.sql.Date
    }

    implicit object sqlDate extends ToJSQLDate[SQLDate] {
      override def toJSQLDate(a: SQLDate): Date = org.apache.spark.sql.catalyst.util.DateTimeUtils.toJavaDate(a.days)
    }

    implicit object sqlTS extends ToJSQLDate[SQLTimestamp] {
      override def toJSQLDate(a: SQLTimestamp): Date = new Date(org.apache.spark.sql.catalyst.util.DateTimeUtils.toJavaTimestamp(a.us).getTime)
    }

    def prop[A](tds: A, monthsToAdd: Int) (implicit encEv : Encoder[A], tEncEv:TypedEncoder[A], isDateType:CatalystDateTime[A], dateTimeEv: ToJSQLDate[A])= {
      val cDS = session.createDataset(
        List(tds).map(dateTimeEv.toJSQLDate)
      ).toDF("ts")
      val resCompare:List[java.sql.Date] = cDS
        .select(org.apache.spark.sql.functions.add_months(cDS("ts"), monthsToAdd))
        .map(
          row => {
            row.getAs[java.sql.Date](0)
          }
        ).collect().toList

      val typedDS = TypedDataset.create(List(tds).map(X1(_)))
      val res = typedDS.select(add_months(typedDS('a), monthsToAdd)).collect().run() //open Question: should this result in java.sql.Date or frameless.SQLDate?
      resCompare ?= res.map(sqlDate.toJSQLDate).toList
    }


    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
  }
}
