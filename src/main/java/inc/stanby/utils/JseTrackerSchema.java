package inc.stanby.utils;

import com.sun.org.apache.xpath.internal.operations.Bool;
import inc.stanby.schema.JseTracker;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JseTrackerSchema implements SerializationSchema<JseTracker>, DeserializationSchema<JseTracker> {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(JseTrackerSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    }

    @Override
    public byte[] serialize(JseTracker event) {
        LOG.info("Serializing node: {}", toJson(event));
        return toJson(event).getBytes();
    }

    @Override
    public boolean isEndOfStream(JseTracker event) {
        return false;
    }

    @Override
    public TypeInformation<JseTracker> getProducedType() {
        return new AvroTypeInfo<>(JseTracker.class);
    }

    public String getStringValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asText();
        }
        return "";
    };

    public Integer getIntegerValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asInt();
        }
        return 0;
    }

    public Long getLongValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asLong();
        }
        return 0L;
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
        return "";
    }


    @Override
    public JseTracker deserialize(byte[] bytes) {
        try {
            ObjectNode node = this.mapper.readValue(bytes, ObjectNode.class);
            LOG.info("Reading node: {}", node.toString());
            String geoLocation = getStringValue("geoLocation", node);
            String cityCode = getStringValue("geoLocation", node);
            String adDistributionId = getStringValue("adDistributionId", node);
            String adJobs = getStringValue("geoLocation", node);
            String bucketType = getStringValue("bucketType", node);
            String category = getStringValue("category", node);
            String clickId = getStringValue("clickId", node);
            String companyName = getStringValue("companyName", node);
            Integer position = getIntegerValue("position", node);
            String documentId = getStringValue("documentId", node);
            String eventType = getStringValue("eventType", node);
            String ip = getStringValue("ip", node);
            String indexType = getStringValue("indexType", node);
            Boolean isAd = getBooleanValue("isAd", node);
            String jobContent = getStringValue("jobContent", node);
            String jobTitle = getStringValue("jobTitle", node);
            String jobType = getStringValue("jobType", node);
            String jobVersionDate = getStringValue("jobVersionDate", node);
            String location = getStringValue("location", node);
            Boolean log_host = getBooleanValue("log_host", node);
            String newJobs = getStringValue("newJobs", node);
            String openDate = getStringValue("openDate", node);
            Integer order = getIntegerValue("order", node);
            String originDocumentId = getStringValue("originDocumentId", node);
            String originalKeyword = getStringValue("originalKeyword", node);
            String referer = getStringValue("referer", node);
            String relatedJobs = getStringValue("relatedJobs", node);
            String role = getStringValue("role", node);
            String salary = getStringValue("salary", node);
            String searchPage = getStringValue("searchPage", node);
            String searchRequestId = getStringValue("searchRequestId", node);
            String searchRequestUrl = getStringValue("searchRequestUrl", node);
            String siteCodes = getStringValue("siteCodes", node);
            String siteName = getStringValue("siteName", node);
            String tag = getStringValue("tag", node);
            String time = getStringValue("time", node);
            Integer totalHits = getIntegerValue("totalHits", node);
            String uaCategory = getStringValue("uaCategory", node);
            String uaValue = getStringValue("uaValue", node);
            String uid = getStringValue("uid", node);
            String updateDate = getStringValue("updateDate", node);
            String visitId = getStringValue("visitId", node);
            String createDateTime = getCreateTime(node);
            if (node.has("address") && node.get("address") != null) {
                LOG.info("Addressxxxxx: {}",node.get("address"));
                if (node.get("address").has("coordinatePoint") && node.get("address").get("coordinatePoint") != null) {
                    LOG.info("Geolocationxxxxx: {}",node.get("address"));
                    String lat = node.get("address").get("coordinatePoint").get("latitude").asText();
                    String lon = node.get("address").get("coordinatePoint").get("longitude").asText();
                    geoLocation = String.format("%s, %s", lat, lon);
                    LOG.info("geoLocationxxxxx: {}", geoLocation);
                };
                if (node.get("address").has("cityCodes") && node.get("address").get("cityCodes") != null) {
                    LOG.info("cityCodesrrrrrrr: {}", node.get("address").get("cityCodes"));
                    cityCode = String.format("%s-%s", "JP", node.get("address").get("cityCodes").asText().substring(0, 2));
                    LOG.info("cityCodexxxx: {}", cityCode);
                }
            }
            return JseTracker
                    .newBuilder()
                    .setAdDistributionId(adDistributionId)
                    .setAdJobs(adJobs)
                    .setBucketType(bucketType)
                    .setCategory(category)
                    .setClickId(clickId)
                    .setCompanyName(companyName)
                    .setPosition(position)
                    .setCreateDateTime(createDateTime)
                    .setDocumentId(documentId)
                    .setEventType(eventType)
                    .setIp(ip)
                    .setIndexType(indexType)
                    .setIsAd(isAd)
                    .setJobContent(jobContent)
                    .setJobTitle(jobTitle)
                    .setJobType(jobType)
                    .setJobVersionDate(jobVersionDate)
                    .setLocation(location)
                    .setLogHost(log_host)
                    .setNewJobs(newJobs)
                    .setOpenDate(openDate)
                    .setOrder(order)
                    .setOriginDocumentId(originDocumentId)
                    .setOriginalKeyword(originalKeyword)
                    .setReferer(referer)
                    .setRelatedJobs(relatedJobs)
                    .setRole(role)
                    .setSalary(salary)
                    .setSearchPage(searchPage)
                    .setSearchRequestId(searchRequestId)
                    .setSearchRequestUrl(searchRequestUrl)
                    .setSiteCodes(siteCodes)
                    .setSiteName(siteName)
                    .setTag(tag)
                    .setTime(time)
                    .setTotalHits(totalHits)
                    .setUaCategory(uaCategory)
                    .setUaValue(uaValue)
                    .setUid(uid)
                    .setUpdateDate(updateDate)
                    .setVisitId(visitId)
                    .setCityCode(cityCode)
                    .setGeoLocation(geoLocation)
                    .setAddress(null)
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);
            return null;
        }
    }

    public static String toJson(JseTracker event) {
        StringBuilder builder = new StringBuilder();

        builder.append("{");
        addTextField(builder, event, "adDistributionId");
        builder.append(", ");
        addTextField(builder, event, "adJobs");
        builder.append(", ");
        addTextField(builder, event, "adRequestId");
        builder.append(", ");
        addTextField(builder, event, "bucketType");
        builder.append(", ");
        addTextField(builder, event, "category");
        builder.append(", ");
        addTextField(builder, event, "clickId");
        builder.append(", ");
        addTextField(builder, event, "companyName");
        builder.append(", ");
        addTextField(builder, event, "position");
        builder.append(", ");
        addTextField(builder, event, "createDateTime");
        builder.append(", ");
        addTextField(builder, event, "createdDateTime");
        builder.append(", ");
        addField(builder, event, "documentId");
        builder.append(", ");
        addTextField(builder, event, "eventType");
        builder.append(", ");
        addTextField(builder, event, "indexType");
        builder.append(", ");
        addField(builder, event, "ip");
        builder.append(", ");
        addTextField(builder, event, "isAd");
        builder.append(", ");
        addTextField(builder, event, "jobContent");
        builder.append(", ");
        addTextField(builder, event, "jobTitle");
        builder.append(", ");
        addTextField(builder, event, "jobType");
        builder.append(", ");
        addTextField(builder, event, "jobVersionDate");
        builder.append(", ");
        addTextField(builder, event, "keyword");
        builder.append(", ");
        addTextField(builder, event, "location");
        builder.append(", ");
        addTextField(builder, event, "log_host");
        builder.append(", ");
        addTextField(builder, event, "newJobs");
        builder.append(", ");
        addTextField(builder, event, "openDate");
        builder.append(", ");
        addTextField(builder, event, "order");
        builder.append(", ");
        addTextField(builder, event, "originDocumentId");
        builder.append(", ");
        addTextField(builder, event, "originalKeyword");
        builder.append(", ");
        addTextField(builder, event, "referer");
        builder.append(", ");
        addTextField(builder, event, "relatedJobs");
        builder.append(", ");
        addTextField(builder, event, "role");
        builder.append(", ");
        addTextField(builder, event, "salary");
        builder.append(", ");
        addTextField(builder, event, "searchPage");
        builder.append(", ");
        addTextField(builder, event, "searchRequestId");
        builder.append(", ");
        addTextField(builder, event, "searchRequestUrl");
        builder.append(", ");
        addTextField(builder, event, "siteCodes");
        builder.append(", ");
        addTextField(builder, event, "siteName");
        builder.append(", ");
        addTextField(builder, event, "tag");
        builder.append(", ");
        addTextField(builder, event, "time");
        builder.append(", ");
        addTextField(builder, event, "totalHits");
        builder.append(", ");
        addTextField(builder, event, "uaCategory");
        builder.append(", ");
        addTextField(builder, event, "uaValue");
        builder.append(", ");
        addTextField(builder, event, "uid");
        builder.append(", ");
        addTextField(builder, event, "updateDate");
        builder.append(", ");
        addTextField(builder, event, "visitId");
        builder.append(", ");
        addTextField(builder, event, "workLocation");
        builder.append(", ");
        addTextField(builder, event, "cityCode");
        builder.append(", ");
        addTextField(builder, event, "geoLocation");
        builder.append("}");

        return builder.toString();
    }

    private static void addField(StringBuilder builder, JseTracker event, String fieldName) {
        addField(builder, fieldName, event.get(fieldName));
    }

    private static void addField(StringBuilder builder, String fieldName, Object value) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append(value);
    }

    private static void addTextField(StringBuilder builder, JseTracker event, String fieldName) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append("\"");
        builder.append(event.get(fieldName));
        builder.append("\"");
    }
}
