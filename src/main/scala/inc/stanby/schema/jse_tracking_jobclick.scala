package inc.stanby.schema

import com.sun.org.apache.xpath.internal.operations.Bool
import org.apache.avro.data.TimeConversions
import org.apache.avro.specific.SpecificData
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object JseTrakingJobClickSchema {
  private val LOG = LoggerFactory.getLogger(classOf[JseTrakingJobClickSchema])

  def toJson(event: JseTracker): String = {
    val builder = new StringBuilder
    builder.append("{")
    addTextField(builder, event, "adDistributionId")
    builder.append(", ")
    addTextField(builder, event, "adJobs")
    builder.append(", ")
    addTextField(builder, event, "adRequestId")
    builder.append(", ")
    addTextField(builder, event, "bucketType")
    builder.append(", ")
    addTextField(builder, event, "category")
    builder.append(", ")
    addTextField(builder, event, "clickId")
    builder.append(", ")
    addTextField(builder, event, "companyName")
    builder.append(", ")
    addTextField(builder, event, "position")
    builder.append(", ")
    addTextField(builder, event, "createDateTime")
    builder.append(", ")
    addTextField(builder, event, "createdDateTime")
    builder.append(", ")
    addField(builder, event, "documentId")
    builder.append(", ")
    addTextField(builder, event, "eventType")
    builder.append(", ")
    addTextField(builder, event, "indexType")
    builder.append(", ")
    addField(builder, event, "ip")
    builder.append(", ")
    addTextField(builder, event, "isAd")
    builder.append(", ")
    addTextField(builder, event, "jobContent")
    builder.append(", ")
    addTextField(builder, event, "jobTitle")
    builder.append(", ")
    addTextField(builder, event, "jobType")
    builder.append(", ")
    addTextField(builder, event, "jobVersionDate")
    builder.append(", ")
    addTextField(builder, event, "keyword")
    builder.append(", ")
    addTextField(builder, event, "location")
    builder.append(", ")
    addTextField(builder, event, "log_host")
    builder.append(", ")
    addTextField(builder, event, "newJobs")
    builder.append(", ")
    addTextField(builder, event, "openDate")
    builder.append(", ")
    addTextField(builder, event, "order")
    builder.append(", ")
    addTextField(builder, event, "originDocumentId")
    builder.append(", ")
    addTextField(builder, event, "originalKeyword")
    builder.append(", ")
    addTextField(builder, event, "referer")
    builder.append(", ")
    addTextField(builder, event, "relatedJobs")
    builder.append(", ")
    addTextField(builder, event, "role")
    builder.append(", ")
    addTextField(builder, event, "salary")
    builder.append(", ")
    addTextField(builder, event, "searchPage")
    builder.append(", ")
    addTextField(builder, event, "searchRequestId")
    builder.append(", ")
    addTextField(builder, event, "searchRequestUrl")
    builder.append(", ")
    addTextField(builder, event, "siteCodes")
    builder.append(", ")
    addTextField(builder, event, "siteName")
    builder.append(", ")
    addTextField(builder, event, "tag")
    builder.append(", ")
    addTextField(builder, event, "time")
    builder.append(", ")
    addTextField(builder, event, "totalHits")
    builder.append(", ")
    addTextField(builder, event, "uaCategory")
    builder.append(", ")
    addTextField(builder, event, "uaValue")
    builder.append(", ")
    addTextField(builder, event, "uid")
    builder.append(", ")
    addTextField(builder, event, "updateDate")
    builder.append(", ")
    addTextField(builder, event, "visitId")
    builder.append(", ")
    addTextField(builder, event, "workLocation")
    builder.append(", ")
    addTextField(builder, event, "cityCode")
    builder.append(", ")
    addTextField(builder, event, "geoLocation")
    builder.append("}")
    builder.toString
  }

  private def addField(builder: StringBuilder, event: JseTracker, fieldName: String): Unit = {
    addField(builder, fieldName, event.get(fieldName))
  }

  private def addField(builder: StringBuilder, fieldName: String, value: Any): Unit = {
    builder.append("\"")
    builder.append(fieldName)
    builder.append("\"")
    builder.append(": ")
    builder.append(value)
  }

  private def addTextField(builder: StringBuilder, event: JseTracker, fieldName: String): Unit = {
    builder.append("\"")
    builder.append(fieldName)
    builder.append("\"")
    builder.append(": ")
    builder.append("\"")
    builder.append(event.get(fieldName))
    builder.append("\"")
  }

  try SpecificData.get.addLogicalTypeConversion(new TimeConversions.TimestampConversion)

}

class JseTrackerSchema extends SerializationSchema[JseTracker] with DeserializationSchema[JseTracker] {
  final private val mapper = new ObjectMapper

  override def serialize(event: JseTracker): Array[Byte] = {
    JseTrackerSchema.LOG.info("Serializing node: {}", JseTrackerSchema.toJson(event))
    JseTrackerSchema.toJson(event).getBytes
  }

  override def isEndOfStream(event: JseTracker) = false

  override def getProducedType = new AvroTypeInfo[JseTracker](classOf[JseTracker])

  def getStringValue(name: String, node: ObjectNode): String = {
    if (node.has(name)) return node.get(name).asText
    ""
  }

  def getIntegerValue(name: String, node: ObjectNode): Integer = {
    if (node.has(name)) return node.get(name).asInt
    0
  }

  def getLongValue(name: String, node: ObjectNode): Long = {
    if (node.has(name)) return node.get(name).asLong
    0L
  }

  def getBooleanValue(name: String, node: ObjectNode): Boolean = {
    if (node.has(name)) return node.get(name).asBoolean
    false
  }

  def getCreateTime(node: ObjectNode): String = {
    if (node.has("createDateTime") && !(node.get("createDateTime").asText == "")) return node.get("createDateTime").asText
    else if (node.has("createdDateTime") && !(node.get("createdDateTime").asText == "")) return node.get("createdDateTime").asText
    ""
  }

  override def deserialize(bytes: Array[Byte]): JseTracker = try {
    val node = this.mapper.readValue(bytes, classOf[ObjectNode])
    JseTrackerSchema.LOG.info("Reading node: {}", node.toString)
    var geoLocation = getStringValue("geoLocation", node)
    var cityCode = getStringValue("geoLocation", node)
    val adDistributionId = getStringValue("adDistributionId", node)
    val adJobs = getStringValue("geoLocation", node)
    val bucketType = getStringValue("bucketType", node)
    val category = getStringValue("category", node)
    val clickId = getStringValue("clickId", node)
    val companyName = getStringValue("companyName", node)
    val position = getIntegerValue("position", node)
    val documentId = getStringValue("documentId", node)
    val eventType = getStringValue("eventType", node)
    val ip = getStringValue("ip", node)
    val indexType = getStringValue("indexType", node)
    val isAd = getBooleanValue("isAd", node)
    val jobContent = getStringValue("jobContent", node)
    val jobTitle = getStringValue("jobTitle", node)
    val jobType = getStringValue("jobType", node)
    val jobVersionDate = getStringValue("jobVersionDate", node)
    val location = getStringValue("location", node)
    val log_host = getBooleanValue("log_host", node)
    val newJobs = getStringValue("newJobs", node)
    val openDate = getStringValue("openDate", node)
    val order = getIntegerValue("order", node)
    val originDocumentId = getStringValue("originDocumentId", node)
    val originalKeyword = getStringValue("originalKeyword", node)
    val referer = getStringValue("referer", node)
    val relatedJobs = getStringValue("relatedJobs", node)
    val role = getStringValue("role", node)
    val salary = getStringValue("salary", node)
    val searchPage = getStringValue("searchPage", node)
    val searchRequestId = getStringValue("searchRequestId", node)
    val searchRequestUrl = getStringValue("searchRequestUrl", node)
    val siteCodes = getStringValue("siteCodes", node)
    val siteName = getStringValue("siteName", node)
    val tag = getStringValue("tag", node)
    val time = getStringValue("time", node)
    val totalHits = getIntegerValue("totalHits", node)
    val uaCategory = getStringValue("uaCategory", node)
    val uaValue = getStringValue("uaValue", node)
    val uid = getStringValue("uid", node)
    val updateDate = getStringValue("updateDate", node)
    val visitId = getStringValue("visitId", node)
    val createDateTime = getCreateTime(node)
    if (node.has("address") && node.get("address") != null) {
      JseTrackerSchema.LOG.info("Addressxxxxx: {}", node.get("address"))
      if (node.get("address").has("coordinatePoint") && node.get("address").get("coordinatePoint") != null) {
        JseTrackerSchema.LOG.info("Geolocationxxxxx: {}", node.get("address"))
        val lat = node.get("address").get("coordinatePoint").get("latitude").asText
        val lon = node.get("address").get("coordinatePoint").get("longitude").asText
        geoLocation = String.format("%s, %s", lat, lon)
        JseTrackerSchema.LOG.info("geoLocationxxxxx: {}", geoLocation)
      }
      else geoLocation = null
      if (node.get("address").has("prefectureCode") && !node.get("address").get("prefectureCode").asText.isEmpty) {
        JseTrackerSchema.LOG.info("cityCodesrrrrrrr node: {}", node.toString)
        JseTrackerSchema.LOG.info("cityCodesrrrrrrr: {}", node.get("address").get("prefectureCode"))
        cityCode = String.format("%s-%s", "JP", node.get("address").get("prefectureCode").asText)
        JseTrackerSchema.LOG.info("cityCodexxxx: {}", cityCode)
      }
      else cityCode = null
    }
    JseTracker.newBuilder.setAdDistributionId(adDistributionId).setAdJobs(adJobs).setBucketType(bucketType).setCategory(category).setClickId(clickId).setCompanyName(companyName).setPosition(position).setCreateDateTime(createDateTime).setDocumentId(documentId).setEventType(eventType).setIp(ip).setIndexType(indexType).setIsAd(isAd).setJobContent(jobContent).setJobTitle(jobTitle).setJobType(jobType).setJobVersionDate(jobVersionDate).setLocation(location).setLogHost(log_host).setNewJobs(newJobs).setOpenDate(openDate).setOrder(order).setOriginDocumentId(originDocumentId).setOriginalKeyword(originalKeyword).setReferer(referer).setRelatedJobs(relatedJobs).setRole(role).setSalary(salary).setSearchPage(searchPage).setSearchRequestId(searchRequestId).setSearchRequestUrl(searchRequestUrl).setSiteCodes(siteCodes).setSiteName(siteName).setTag(tag).setTime(time).setTotalHits(totalHits).setUaCategory(uaCategory).setUaValue(uaValue).setUid(uid).setUpdateDate(updateDate).setVisitId(visitId).setCityCode(cityCode).setGeoLocation(geoLocation).setAddress(null).build
  } catch {
    case e: Exception =>
      JseTrackerSchema.LOG.warn("Failed to serialize event: {}", new String(bytes), e)
      null
  }
}
