package inc.stanby.serializers;

import inc.stanby.schema.StanbyEvent;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua_parser.Parser;
import ua_parser.Client;

import java.text.SimpleDateFormat;
import java.util.Date;

public class StanbyEventDeserializationSchema implements DeserializationSchema<StanbyEvent> {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(StanbyEventDeserializationSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    }

    @Override
    public boolean isEndOfStream(StanbyEvent event) {
        return false;
    }

    @Override
    public TypeInformation<StanbyEvent> getProducedType() {
        return new AvroTypeInfo<>(StanbyEvent.class);
    }

    private boolean checkYahoo(String event_type, String page, String area, String current_url) {
        return (event_type.equals("link") && page.equals("search") && area.equals("card") && current_url.matches(".*sr_fr.*"));
    }

    public String getStringValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asText();
        }
        return null;
    };

    public Integer getIntegerValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asInt();
        }
        return null;
    }

    public Long getLongValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asLong();
        }
        return null;
    }

    public Boolean getBooleanValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asBoolean();
        }
        return false;
    }

    public String getCreateTime(ObjectNode node) {
        if (node.has("createDateTime") && !node.get("createDateTime").asText().equals("")) {
            return node.get("createDateTime").asText();
        } else
        if (node.has("createdDateTime") && !node.get("createdDateTime").asText().equals("")) {
            return node.get("createdDateTime").asText();
        }
        return null;
    }

    @Override
    public StanbyEvent deserialize(byte[] bytes) {
        try {
            ObjectNode node = this.mapper.readValue(bytes, ObjectNode.class);
            LOG.info("Reading node: {}", node.toString());
            String service = getStringValue("service", node);
            String event_type = getStringValue("event_type", node);
            String suid = getStringValue("suid", node);
            String ssid = getStringValue("ssid", node);
            String current_url = getStringValue("current_url", node);
            String referrer = getStringValue("referrer", node);
            String page = getStringValue("page", node);
            String page_type = getStringValue("page_type", node);
            String user_agent = getStringValue("user_agent", node);
            String search_request_id = getStringValue("search_request_id", node);
            long epoch = getLongValue("epoch", node);
            String ip = getStringValue("ip", node);
            String area = getStringValue("area", node);
            String element = getStringValue("element", node);
            Parser uaParser = new Parser();
            Client c = uaParser.parse(user_agent.toString());
            String ua_os = String.format("%s_%s", c.os.family, c.os.major);
            String ua_device = c.device.family;
            String ua_family = c.userAgent.family;
            boolean fromYahoo = checkYahoo(event_type, page, area, current_url);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            String createDateTime = sdf.format(new Date(epoch));
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
                    .setArea(area)
                    .setElement(element)
                    .setFromYahoo(fromYahoo)
                    .setUaDevice(ua_os)
                    .setUaOs(ua_device)
                    .setUaFamily(ua_family)
                    .setCreateDateTime(createDateTime)
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);

            return null;
        }
    }
}
