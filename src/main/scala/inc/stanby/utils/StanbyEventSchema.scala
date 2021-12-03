package inc.stanby.utils;

import inc.stanby.schema.StanbyEvent;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.HashMap;
import ua_parser.Parser;
import ua_parser.Client;

class StanbyEventSchema extends SerializationSchema[StanbyEvent] with DeserializationSchema[StanbyEvent] {

  var mapper = new ObjectMapper;
  var LOG = LoggerFactory.getLogger(this.getClass.getName);

  SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());

  override def serialize(event: StanbyEvent): Array[Byte] = {
    LOG.info("Serializing node: {}", toJson(event));
    return toJson(event).getBytes();
  }

  override def isEndOfStream(event: StanbyEvent): Boolean = {
    return false;
  }

  override def getProducedType(): TypeInformation[StanbyEvent] = {
    return new AvroTypeInfo(classOf[StanbyEvent]);
  }

  override def deserialize(bytes: Array[Byte]): StanbyEvent = {
    try {
      var node = this.mapper.readValue(bytes, classOf[ObjectNode]);
      LOG.info("Reading node: {}", node.toString);
      var service = "";
      var event_type = "";
      var suid = "";
      var ssid = "";
      var current_url = "";
      var referrer = "";
      var page = "";
      var page_type = "";
      var user_agent = "";
      var search_request_id = "";
      var epoch = 0L;
      var ip = "";

      if (node.has("service")) service = node.get("service").asText();
      if (node.has("event_type")) event_type = node.get("event_type").asText();
      if (node.has("suid")) suid = node.get("suid").asText();
      if (node.has("ssid")) ssid = node.get("ssid").asText();
      if (node.has("current_url")) current_url = node.get("current_url").asText();
      if (node.has("referrer")) referrer = node.get("referrer").asText();
      if (node.has("page")) page = node.get("page").asText();
      if (node.has("page_type")) page_type = node.get("page_type").asText();
      if (node.has("user_agent")) user_agent = node.get("user_agent").asText();
      if (node.has("search_request_id")) search_request_id = node.get("search_request_id").asText();
      if (node.has("epoch")) epoch = node.get("epoch").asLong();
      if (node.has("ip")) ip = node.get("ip").asText();
      var uaParser = new Parser();
      var c = uaParser.parse(user_agent.toString());
      var ua_os = String.format("%s_%s", c.os.family, c.os.major);
      var ua_device = c.device.family;
      var ua_family = c.userAgent.family;
      return StanbyEvent
          .newBuilder()
          .setService(service)
          .setEventType(event_type)
          .setSuid(suid)
          .setSsid(ssid)
          .setCurrentUrl(current_url)
          .setReferrer(referrer)
          .setPage(page)
          .setPageType(page_type)
          .setUserAgent(user_agent)
          .setSearchRequestId(search_request_id)
          .setEpoch(epoch)
          .setIp(ip)
          .setUaDevice(ua_os)
          .setUaOs(ua_device)
          .setUaFamily(ua_family)
          .build();
    } catch {
      case e: Exception => LOG.warn(s"Failed to serialize event: $e");
      return null;
    }
  }

  def toJson(event: StanbyEvent): String = {
    val builder = new StringBuilder();

    builder.append("{");
    addTextField(builder, event, "service");
    builder.append(", ");
    addTextField(builder, event, "event_type");
    builder.append(", ");
    addTextField(builder, event, "suid");
    builder.append(", ");
    addTextField(builder, event, "ssid");
    builder.append(", ");
    addTextField(builder, event, "current_url");
    builder.append(", ");
    addTextField(builder, event, "referrer");
    builder.append(", ");
    addTextField(builder, event, "page");
    builder.append(", ");
    addTextField(builder, event, "page_type");
    builder.append(", ");
    addTextField(builder, event, "user_agent");
    builder.append(", ");
    addTextField(builder, event, "search_request_id");
    builder.append(", ");
    addField(builder, event, "epoch");
    builder.append(", ");
    addTextField(builder, event, "ip");
    builder.append(", ");
    addTextField(builder, event, "ua_os");
    builder.append(", ");
    addField(builder, event, "ua_device");
    builder.append(", ");
    addTextField(builder, event, "ua_family");
    builder.append("}");

    return builder.toString();
  }

  def addField(builder: StringBuilder, event: StanbyEvent, fieldName: String) {
    addField(builder, fieldName, event.get(fieldName));
  }

  def addField(builder: StringBuilder, fieldName: String, value: Object) {
    builder.append("\"");
    builder.append(fieldName);
    builder.append("\"");

    builder.append(": ");
    builder.append(value);
  }

  def addTextField(builder: StringBuilder, event: StanbyEvent, fieldName: String) {
    builder.append("\"");
    builder.append(fieldName);
    builder.append("\"");

    builder.append(": ");
    builder.append("\"");
    builder.append(event.get(fieldName));
    builder.append("\"");
  }
}
