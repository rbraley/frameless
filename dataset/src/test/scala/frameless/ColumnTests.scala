package frameless

import org.scalacheck.{Gen, Prop}
import org.scalacheck.Prop._

import scala.math.Ordering.Implicits._
import scala.util.Try

class ColumnTests extends TypedDatasetSuite {

  test("select('a < 'b, 'a <= 'b, 'a > 'b, 'a >= 'b)") {
    def prop[A: TypedEncoder : frameless.CatalystOrdered : scala.math.Ordering](a: A, b: A): Prop = {
      val dataset = TypedDataset.create(X2(a, b) :: Nil)
      val A = dataset.col('a)
      val B = dataset.col('b)

      val dataset2 = dataset.selectMany(
        A < B, A < b,   // One test uses columns, other uses literals
        A <= B, A <= b,
        A > B, A > b,
        A >= B, A >= b
      ).collect().run().toVector

      dataset2 ?= Vector((a < b, a < b, a <= b, a <= b, a > b, a > b, a >= b, a >= b))

    }

    implicit val sqlDateOrdering: Ordering[SQLDate] = Ordering.by(_.days)
    implicit val sqlTimestmapOrdering: Ordering[SQLTimestamp] = Ordering.by(_.us)

    check(forAll(prop[Int] _))
    check(forAll(prop[Boolean] _))
    check(forAll(prop[Byte] _))
    check(forAll(prop[Short] _))
    check(forAll(prop[Long] _))
    check(forAll(prop[Float] _))
    check(forAll(prop[Double] _))
    check(forAll(prop[SQLDate] _))
    check(forAll(prop[SQLTimestamp] _))
    check(forAll(prop[String] _))
  }

  test("toString") {
    val t = TypedDataset.create((1,2)::Nil)
    t('_1).toString ?= t.dataset.col("_1").toString()
  }

  test("getItemSimple") {
    case class ListsAndIndexOutOfBounds[A](xss: List[List[A]], index: Int)

    def prop[A : TypedEncoder ](both: ListsAndIndexOutOfBounds[A]) = {
      import TypedColumn._

      val ListsAndIndexOutOfBounds(value, index) = both
      val itemsAtIndex: List[Option[A]] = value.map(xs => Try(xs.apply(index)).toOption)
      val ds: TypedDataset[X1[List[A]]] = TypedDataset.create(value.map(X1.apply))
      val col: TypedColumn[X1[List[A]], List[A]] = ds('a)
      val newDS  = ds.select(col.getItem(index))
      val (nones,somes) = itemsAtIndex.span(_.isEmpty)
      println(s"nones: ${nones.length}\t somes: ${somes.length}")
      itemsAtIndex ?= newDS.collect().run().toList
    }

    def myGen[T](implicit gen: Gen[T]) = for {
      size <- Gen.choose(0,20)
      offset <- Gen.choose(0, 10)
      index <- Gen.choose(0, size + 20)
      xss <- Gen.listOf(Gen.listOfN(size + offset , gen))
    } yield ListsAndIndexOutOfBounds(xss, index)

    check(forAll(myGen[Int](Gen.choose(0,10)))(prop[Int] _))
  }


}
