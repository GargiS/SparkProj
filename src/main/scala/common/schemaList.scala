package common

trait schemaList {

  case class FactTable(UserId: Int, event_date: String, web_pageId: Int)

  case class LookUpTable(web_pageId: Int, webPage_Type: String)

  case class InputConfig(pageType: Array[String], metricType: Array[String], timeWindow: Array[String], dateOfReference: String)

}
