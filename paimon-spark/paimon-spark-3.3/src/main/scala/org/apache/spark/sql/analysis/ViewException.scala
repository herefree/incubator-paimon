package org.apache.spark.sql.analysis

class ViewException {

}
class NoSuchViewException(errorClass: String, messageParameters: Map[String, String])
  extends Exception {
}
class ViewAlreadyExistsException(errorClass: String, messageParameters: Map[String, String])
  extends Exception {
}
