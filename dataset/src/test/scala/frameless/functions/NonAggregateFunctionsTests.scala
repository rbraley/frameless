package frameless
package functions

import frameless.functions.nonAggregate._
import org.apache.spark.sql.Encoder
import TypedEncoderBinaryType._
import org.scalacheck.Gen
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


  test("array_contains"){


    val spark = session
    import spark.implicits._

    val listLength = 10
    val idxs = Stream.continually(Range(0, listLength)).flatten.toIterator

    def prop(values: List[Int], shouldBeIn:Boolean) = {

      val contained = if (shouldBeIn) values(idxs.next) else -1

      val cDS = session.createDataset(List(values))
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.array_contains(cDS("value"), contained))
        .map(_.getAs[Boolean](0))
        .collect().toList


      val typedDS = TypedDataset.create(List(X1(values)))
      val res = typedDS
        .select(array_contains(typedDS('a), contained))
        .collect()
        .run()
        .toList

      resCompare ?= res
    }


    check(
      forAll(
        Gen.listOfN(listLength, Gen.choose(0,100)),
        Gen.oneOf(true,false)
      )
      (prop)
    )
  }

  test("ascii"){
    val spark = session
    import spark.implicits._

    def prop(values:List[String]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.ascii(cDS("value")))
        .map(_.getAs[Int](0))
        .collect().toList

      val typedDS = TypedDataset.create(values.map(X1(_)))
      val res = typedDS
        .select(ascii(typedDS('a)))
        .collect()
        .run()
        .toList

      resCompare ?= res
    }

    check(forAll(prop _))
  }


  test("atan") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[A]) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan(cDS("value")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value.map(X1(_)))
      val res = typedDS
        .select(atan(typedDS('a)))
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

  test("asin") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[A]) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.asin(cDS("value")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value.map(X1(_)))
      val res = typedDS
        .select(asin(typedDS('a)))
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

  test("atan2") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder, B: CatalystNumeric : TypedEncoder : Encoder](value: List[X2[A,B]])
            (implicit encEv: Encoder[X2[A,B]]) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan2(cDS("a"), cDS("b")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value)
      val res = typedDS
        .select(atan2(typedDS('a), typedDS('b)))
        .deserialized
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect()
        .run()
        .toList

      resCompare ?= res
    }


    check(forAll(prop[Int, Long] _))
    check(forAll(prop[Long, Int] _))
    check(forAll(prop[Short, Byte] _))
    check(forAll(prop[BigDecimal, Double] _))
    check(forAll(prop[Byte, Int] _))
    check(forAll(prop[Double, Double] _))
  }

  test("atan2LitLeft") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[A], lit:Double) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan2(lit, cDS("value")))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value.map(X1(_)))
      val res = typedDS
        .select(atan2(lit, typedDS('a)))
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

  test("atan2LitRight") {
    val spark = session
    import spark.implicits._

    def prop[A: CatalystNumeric : TypedEncoder : Encoder](value: List[A], lit:Double) = {
      val cDS = session.createDataset(value)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.atan2(cDS("value"), lit))
        .map(_.getAs[Double](0))
        .map(DoubleBehaviourUtils.nanNullHandler)
        .collect().toList


      val typedDS = TypedDataset.create(value.map(X1(_)))
      val res = typedDS
        .select(atan2(typedDS('a), lit))
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

  test("base64") {
    val spark = session
    import spark.implicits._



    def prop(values:List[Array[Byte]]) = {
      val cDS = session.createDataset(values)
      val resCompare = cDS
        .select(org.apache.spark.sql.functions.base64(cDS("value")))
        .map(_.getAs[String](0))
        .collect().toList

      val typedDS = TypedDataset.create(values.map(X1(_)))
      val res = typedDS
        .select(base64(typedDS('a)))
        .collect()
        .run()
        .toList

      resCompare ?= res
    }

    check(forAll(prop _))
  }



}
