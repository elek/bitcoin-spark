package net.anzix.hadoop

trait JobConfiguration {
  def localMode: Boolean

  def outputDir: String
}

case class DefaultJobConfiguration(override val localMode: Boolean = false, override val outputDir: String) extends JobConfiguration