package frameless

trait CatalystCollection[C[_]]

object CatalystCollection {
  implicit object arrayObject extends CatalystCollection[Array]
  implicit object seqObject extends CatalystCollection[Seq]
  implicit object listObject extends CatalystCollection[List]
  implicit object vectorObject extends CatalystCollection[Vector]
  implicit object setObject extends CatalystCollection[Set]
}
