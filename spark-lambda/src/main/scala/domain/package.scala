package object domain {
  case class Activity(timestamp_hour: Long,
                      referrer: String,
                      action: String,
                      prevPage: String,
                      visitor: String,
                      page: String,
                      product: String,
                      inputProps: Map[String, String] = Map()
                     )
}
