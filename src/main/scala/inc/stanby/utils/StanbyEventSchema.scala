package inc.stanby.utils

import inc.stanby.schema.StanbyEvent
import org.apache.avro.data.TimeConversions
import org.apache.avro.specific.SpecificData
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.joda.time.DateTime
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util
import ua_parser.Parser
import ua_parser.Client


object StanbyEventSchema {
  private val LOG = LoggerFactory.getLogger(classOf[StanbyEventSchema])

  def toJson(event: StanbyEvent): String = {
    val builder = new StringBuilder
    builder.append("{")
    addTextField(builder, event, "service")
    builder.append(", ")
    addTextField(builder, event, "event_type")
    builder.append(", ")
    addTextField(builder, event, "suid")
    builder.append(", ")
    addTextField(builder, event, "ssid")
    builder.append(", ")
    addTextField(builder, event, "current_url")
    builder.append(", ")
    addTextField(builder, event, "referrer")
    builder.append(", ")
    addTextField(builder, event, "page")
    builder.append(", ")
    addTextField(builder, event, "page_type")
    builder.append(", ")
    addTextField(builder, event, "user_agent")
    builder.append(", ")
    addTextField(builder, event, "search_request_id")
    builder.append(", ")
    addField(builder, event, "epoch")
    builder.append(", ")
    addTextField(builder, event, "ip")
    builder.append(", ")
    addTextField(builder, event, "ua_os")
    builder.append(", ")
    addField(builder, event, "ua_device")
    builder.append(", ")
    addTextField(builder, event, "ua_family")
    builder.append("}")
    builder.toString
  }

  private def addField(builder: StringBuilder, event: StanbyEvent, fieldName: String): Unit = {
    addField(builder, fieldName, event.get(fieldName))
  }

  private def addField(builder: StringBuilder, fieldName: String, value: Any): Unit = {
    builder.append("\"")
    builder.append(fieldName)
    builder.append("\"")
    builder.append(": ")
    builder.append(value)
  }

  private def addTextField(builder: StringBuilder, event: StanbyEvent, fieldName: String): Unit = {
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

class StanbyEventSchema extends SerializationSchema[StanbyEvent] with DeserializationSchema[StanbyEvent] {
  final private val mapper = new ObjectMapper

  override def serialize(event: StanbyEvent): Array[Byte] = {
    StanbyEventSchema.LOG.info("Serializing node: {}", StanbyEventSchema.toJson(event))
    StanbyEventSchema.toJson(event).getBytes
  }

  override def isEndOfStream(event: StanbyEvent) = false

  override def getProducedType = new AvroTypeInfo[StanbyEvent](classOf[StanbyEvent])

  override def deserialize(bytes: Array[Byte]): StanbyEvent = try {
    val node = this.mapper.readValue(bytes, classOf[ObjectNode])
    StanbyEventSchema.LOG.info("Reading node: {}", node.toString)
    val uaParser = new Parser
    val user_agent = node.get("user_agent").asText
    val c = uaParser.parse(user_agent)
    val ua_os = String.format("%s_%s", c.os.family, c.os.major)
    val ua_device = c.device.family
    val ua_family = c.userAgent.family
    StanbyEvent
      .newBuilder()
      .setService(node.get("service").asText())
      .setEventType(node.get("event_type").asText())
      .setSuid(node.get("suid").asText())
      .setSsid(node.get("ssid").asText())
      .setCurrentUrl(node.get("current_url").asText())
      .setReferrer(node.get("referrer").asText())
      .setPage(node.get("page").asText())
      .setPageType(node.get("page_type").asText())
      .setUserAgent(node.get("user_agent").asText())
      .setSearchRequestId(node.get("search_request_id").asText())
      .setEpoch(node.get("epoch").asLong())
      .setIp(node.get("ip").asText())
      .setUaDevice(ua_os)
      .setUaOs(ua_device)
      .setUaFamily(ua_family)
      .build();
  } catch {
    case e: Exception =>
      StanbyEventSchema.LOG.warn("Failed to serialize event: {}", e)
      null
  }
}
