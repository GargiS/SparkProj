package common

object schemaList {

  case class FactTable(USER_ID: Int, EVENT_DATE: String, WEB_PAGEID: Int)

  case class LookUpTable(WEB_PAGEID: Int, WEBPAGE_TYPE: String)

  case class InputConfig(pageType: Array[String], metricType: Array[String], timeWindow: Array[String], dateOfReference: String)

}
