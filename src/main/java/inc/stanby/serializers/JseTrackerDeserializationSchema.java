package inc.stanby.serializers;

import inc.stanby.schema.JseTracker;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JseTrackerDeserializationSchema implements DeserializationSchema<JseTracker> {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(JseTrackerDeserializationSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
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
    public JseTracker deserialize(byte[] bytes) {
        try {
            ObjectNode node = this.mapper.readValue(bytes, ObjectNode.class);
            LOG.info("Reading node: {}", node.toString());
            String geoLocation = getStringValue("geoLocation", node);
            String cityCode = getStringValue("citycode", node);
            String station = getStringValue("station", node);
            String salaryUnit = getStringValue("salaryUnit", node);
            Long salaryMin = getLongValue("salaryMin", node);
            Long salaryMax = getLongValue("salaryMax", node);
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
            Integer searchPage = getIntegerValue("searchPage", node);
            String searchRequestId = getStringValue("searchRequestId", node);
            String searchRequestUrl = getStringValue("searchRequestUrl", node);
            String requestUrl = getStringValue("requestUrl", node);
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
            if (node.has("address") && !node.get("address").isEmpty()) {
                LOG.info("Addressxxxxx: {}",node.get("address"));
                if (node.get("address").has("coordinatePoint") && !node.get("address").get("coordinatePoint").isEmpty()) {
                    LOG.info("Geolocationxxxxx: {}",node.get("address"));
                    String lat = node.get("address").get("coordinatePoint").get("latitude").asText();
                    String lon = node.get("address").get("coordinatePoint").get("longitude").asText();
                    geoLocation = String.format("%s, %s", lat, lon);
                    LOG.info("geoLocationxxxxx: {}", geoLocation);
                } else {
                    geoLocation = null;
                }
                if (node.get("address").has("prefectureCode") && !node.get("address").get("prefectureCode").isEmpty()) {
                    cityCode = String.format("%s-%s", "JP", node.get("address").get("prefectureCode").asText());
                } else {
                    cityCode = null;
                }
                if (cityCode == null && node.get("address").has("cityCodes") && !node.get("address").get("cityCodes").isEmpty()) {
                    cityCode = String.format("%s-%s", "JP", node.get("address").get("cityCodes").get(0).asText().replace("\"", "").substring(0, 2));
                }
                if (node.get("address").has("station") && !node.get("address").get("station").asText().isEmpty()) {
                    station = node.get("address").get("station").asText();
                } else {
                    station = null;
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
                    .setSearchPage(searchPage)
                    .setSearchRequestId(searchRequestId)
                    .setSearchRequestUrl(searchRequestUrl)
                    .setRequestUrl(requestUrl)
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
                    .setStation(station)
                    .setGeoLocation(geoLocation)
                    .setAddress(null)
                    .setSalary(null)
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);
            return null;
        }
    }
}
