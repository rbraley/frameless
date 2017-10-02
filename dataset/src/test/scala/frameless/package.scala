import org.scalacheck.{Arbitrary, Gen}

package object frameless {
  /** Fixed decimal point to avoid precision problems specific to Spark */
  implicit val arbBigDecimal: Arbitrary[BigDecimal] = Arbitrary {
    for {
      x <- Gen.chooseNum(-1000, 1000)
      y <- Gen.chooseNum(0, 1000000)
    } yield BigDecimal(s"$x.$y")
  }

  /** Fixed decimal point to avoid precision problems specific to Spark */
  implicit val arbDouble: Arbitrary[Double] = Arbitrary {
    arbBigDecimal.arbitrary.map(_.toDouble)
  }

  implicit val arbSqlDate = Arbitrary {
    Arbitrary.arbitrary[Int].map(SQLDate)
  }

  implicit val arbSqlTimestamp = Arbitrary {
    Arbitrary.arbitrary[Long].map(SQLTimestamp)
  }

  implicit def arbTuple1[A: Arbitrary] = Arbitrary {
    Arbitrary.arbitrary[A].map(Tuple1(_))
  }

  // see issue with scalacheck non serializable Vector: https://github.com/rickynils/scalacheck/issues/315
  implicit def arbVector[A](implicit A: Arbitrary[A]): Arbitrary[Vector[A]] =
    Arbitrary(Gen.listOf(A.arbitrary).map(_.toVector))

  implicit val arbUdtEncodedClass: Arbitrary[UdtEncodedClass] = Arbitrary {
    for {
      int <- Arbitrary.arbitrary[Int]
      doubles <- Gen.listOf(arbDouble.arbitrary)
    } yield new UdtEncodedClass(int, doubles.toArray)
  }

}
