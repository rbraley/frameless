package frameless
package functions

import frameless.functions.nonAggregate._
import org.apache.spark.sql.Encoder
import org.scalacheck.Prop._

class NonAggregateFunctionsTests extends TypedDatasetSuite {

  val spark = session
  import spark.implicits._



  test("add_months"){
    def prop[A: CatalystDateTime : TypedEncoder : Encoder](tds: A, monthsToAdd: Int) = {
      val cDS = session.createDataset(
        List(tds).map(CatalystDateTime[A].toJavaSQLDate)
      ).toDF("ts")
      val resCompare:List[java.sql.Date] = cDS
        .select(org.apache.spark.sql.functions.add_months(cDS("ts"), monthsToAdd))
        .map(_.getAs[java.sql.Date](0))
        .collect().toList

      val typedDS = TypedDataset.create(List(tds).map(X1(_)))
      val res = typedDS.select(add_months(typedDS('a), monthsToAdd)).collect().run()
      resCompare ?= res.map(CatalystDateTime[SQLDate].toJavaSQLDate).toList
    }


    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
  }



  test("abs") {

    def prop[A: CatalystNumeric : TypedEncoder](value: A) (implicit encEv: Encoder[A]) = {
      val cDS = session.createDataset(List(value))
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.abs(cDS("value")))
        .map(_.getAs[A](0))
        .collect().toList


      val typedDS = TypedDataset.create(List(value).map(X1(_)))
      val res = typedDS.select(abs(typedDS('a))).collect().run().toList

      resCompare ?= res
    }


    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    //check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("acos") {
    def prop[A: CatalystNumeric : TypedEncoder](value: A) (implicit encEv: Encoder[A]) = {
      val cDS = session.createDataset(List(value))
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.acos(cDS("value")))
        .map(_.getAs[Double](0))
        .collect().toList


      val typedDS = TypedDataset.create(List(value).map(X1(_)))
      val res = typedDS.select(acos(typedDS('a))).collect().run().toList

      resCompare ?= res
    }


    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[BigDecimal] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }



}
