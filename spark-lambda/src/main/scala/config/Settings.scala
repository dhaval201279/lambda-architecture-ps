package config

import com.typesafe.config.ConfigFactory

/**
  * Scala provides 'object' to create a singleton object of a given class
  *
  * similar to static classes in java
  * */
object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("clickstream")

    /** dont want scala to evaluate the value of these properties, want to evaluate when it is being used */
    lazy val records = weblogGen.getInt("records")
    lazy val timeMultiplier = weblogGen.getInt("time_multiplier")
    lazy val pages = weblogGen.getInt("pages")
    lazy val visitors = weblogGen.getInt("visitors")
    lazy val filePath = weblogGen.getString("file_path")

  }

}
