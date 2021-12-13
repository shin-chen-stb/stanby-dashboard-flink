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

    @Override
    public StanbyEvent deserialize(byte[] bytes) {
        try {
            ObjectNode node = this.mapper.readValue(bytes, ObjectNode.class);
            LOG.info("Reading node: {}", node.toString());
            String service = "";
            String event_type = "";
            String suid = "";
            String ssid = "";
            String current_url = "";
            String referrer = "";
            String page = "";
            String page_type = "";
            String user_agent = "";
            String search_request_id = "";
            long epoch = 0L;
            String ip = "";
            String area = "";
            String element = "";

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
            if (node.has("area")) area = node.get("area").asText();
            if (node.has("element")) element = node.get("element").asText();
            Parser uaParser = new Parser();
            Client c = uaParser.parse(user_agent.toString());
            String ua_os = String.format("%s_%s", c.os.family, c.os.major);
            String ua_device = c.device.family;
            String ua_family = c.userAgent.family;
            boolean fromYahoo = checkYahoo(event_type, page, area, current_url);
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
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);

            return null;
        }
    }
}
