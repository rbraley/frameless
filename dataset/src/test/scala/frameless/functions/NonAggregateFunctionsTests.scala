package frameless
package functions

import frameless.functions.nonAggregate._
import org.apache.spark.sql.Encoder
import org.scalacheck.Prop._

class NonAggregateFunctionsTests extends TypedDatasetSuite {


  test("add_months"){
    val spark = session
    import spark.implicits._

    def prop[A: CatalystDateTime : TypedEncoder : Encoder](tds: List[A], monthsToAdd: Int) = {
      val cDS = session.createDataset(
        tds.map(CatalystDateTime[A].toJavaSQLDate)
      ).toDF("ts")
      val resCompare:List[java.sql.Date] = cDS
        .select(org.apache.spark.sql.functions.add_months(cDS("ts"), monthsToAdd))
        .map(_.getAs[java.sql.Date](0))
        .collect().toList

      val typedDS = TypedDataset.create(tds.map(X1(_)))
      val res = typedDS.select(add_months(typedDS('a), monthsToAdd)).collect().run()
      resCompare ?= res.map(CatalystDateTime[SQLDate].toJavaSQLDate).toList
    }


    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
  }



  test("abs") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[A])= {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.abs(cDS("value")))
        .map(_.getAs[A](0))
        .collect().toList


      val typedDS = TypedDataset.create(value.map(X1(_)))
      val res = typedDS
        .select(abs(typedDS('a)))
        .collect()
        .run()
        .toList

      resCompare ?= res
    }


    check(forAll(prop[Int] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Short] _))
    //check(forAll(prop[BigDecimal] _)) untyped abs implementation will convert this to java.math.BigDecimal which results in class cast exceptions.
    check(forAll(prop[Byte] _))
    check(forAll(prop[Double] _))
  }

  test("acos") {
    //this has to be in each test case. if it is put on the class definition there will be magical null pointer exceptions.
    //this renaming is also necessary or there will be errors. This needs someone smarter than me to figure out.
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[A]) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.acos(cDS("value")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value.map(X1(_)))
      val res = typedDS
        .select(acos(typedDS('a)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

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
