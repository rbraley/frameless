package frameless


trait CatalystDateTime[A]

object CatalystDateTime{
  implicit object sqlDate extends CatalystDateTime[SQLDate]
  implicit object sqlTimeStamp extends CatalystDateTime[SQLTimestamp]

}
