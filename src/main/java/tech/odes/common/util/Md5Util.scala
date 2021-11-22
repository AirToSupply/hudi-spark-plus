package tech.odes.common.util

/**
 * 2021-11-09 Town(airtosupply@126.com)
 */
object Md5Util {
  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
    "%02x".format(_)
  }.foldLeft("") {
    _ + _
  }
}
