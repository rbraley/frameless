package frameless
package functions

import frameless.functions.nonAggregate._
import org.scalacheck.Prop._
import org.apache.spark.sql.Encoder

class NonAggregateFunctionsTests extends TypedDatasetSuite {

  test("add_months"){
    val spark = session
    import spark.implicits._

    def prop[A: TypedEncoder](tds: A, monthsToAdd: Int) (implicit encEv : Encoder[A])= {
      val cDS = session.createDataset(List(tds))
      val resCompare:A = cDS
        .select(org.apache.spark.sql.functions.add_months(cDS("value"), monthsToAdd))
        .map(
          row => {
            row.getAs[A](0)
          }
        ).collect().head

      val typedDS = TypedDataset.create(List(tds))
      val res = typedDS.select(add_months(typedDS('_1), monthsToAdd)).collect().run().head

      resCompare ?= res
    }

    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
  }
}
