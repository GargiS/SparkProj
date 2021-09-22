package common

object schemaList {

  /** case class for input parameters */
  case class InputConfig(pageType: Array[String], metricType: Array[String], timeWindow: Array[String], dateOfReference: String)

  /** case class defined for Fact Table **/
  case class FactTable(USER_ID: Int, EVENT_DATE: String, WEB_PAGEID: Int)

  /** case class defined for Lookup Table */
  case class LookUpTable(WEB_PAGEID: Int, WEBPAGE_TYPE: String)



}
