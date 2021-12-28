package inc.stanby.serializers;

import inc.stanby.schema.AdTracking;
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

public class AdTrackingDeserializationSchema implements DeserializationSchema<AdTracking> {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(AdTrackingDeserializationSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    }


    @Override
    public boolean isEndOfStream(AdTracking event) {
        return false;
    }

    @Override
    public TypeInformation<AdTracking> getProducedType() {
        return new AvroTypeInfo<AdTracking>(AdTracking.class);
    }

    public String getStringValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asText();
        }
        return "";
    };

    public String getPrefectureCode(String name, ObjectNode node) {
        if (node.has(name)) {
            return "JP-" + node.get(name).asText();
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

    public Double getDoubleValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asDouble();
        }
        return 0.0;
    }

    public Boolean getBooleanValue(String name, ObjectNode node) {
        if (node.has(name)) {
            return node.get(name).asBoolean();
        }
        return false;
    }

    public ObjectNode getChildeNode(String name, ObjectNode node) {
        if (node.has(name)) {
            return (ObjectNode) node.get(name);
        }
        return node;
    }

    public String getCreateTime(ObjectNode node) {
        if (node.has("createDateTime") && !node.get("createDateTime").asText().isEmpty()) {
            return node.get("createDateTime").asText();
        } else
        if (node.has("createdDateTime") && !node.get("createdDateTime").asText().isEmpty()) {
            return node.get("createdDateTime").asText();
        }
        return "";
    }


    @Override
    public AdTracking deserialize(byte[] bytes) {
        try {
            ObjectNode node = this.mapper.readValue(bytes, ObjectNode.class);
            ObjectNode msgNode = getChildeNode("msg", node);
            ObjectNode salaryNode = getChildeNode("salary", msgNode);
            ObjectNode requestConditionNode = getChildeNode("requestCondition", msgNode);
            ObjectNode AddressNode = getChildeNode("address", requestConditionNode);

            LOG.info("Reading node: {}", node.toString());
            String category = getStringValue("category", node);
            String role = getStringValue("role", node);
            String log_host = getStringValue("log_host", node);
            String tag = getStringValue("tag", node);
            String time = getStringValue("time", node);
            String logType = getStringValue("logType", msgNode);
            String distributionId = getStringValue("distributionId", msgNode);
            String distributionDateTime = getStringValue("distributionDateTime", msgNode);
            String requestId = getStringValue("requestId", msgNode);
            String uid = getStringValue("uid", msgNode);
            String visitId = getStringValue("visitId", msgNode);
            String uaValue = getStringValue("uaValue", msgNode);
            String ip = getStringValue("ip", msgNode);
            String deviceType = getStringValue("deviceType", msgNode);
            Boolean isRandom = getBooleanValue("isRandom", msgNode);
            String publisherId = getStringValue("publisherId", msgNode);
            String publisherChannelId = getStringValue("publisherChannelId", msgNode);
            String publisherChannelType = getStringValue("publisherChannelType", msgNode);
            String bucketType = getStringValue("bucketType", msgNode);
            String documentId = getStringValue("documentId", msgNode);
            String documentUrl = getStringValue("documentUrl", msgNode);
            String siteCode = getStringValue("siteCode", msgNode);
            String documentTitle = getStringValue("documentTitle", msgNode);
            String documentContent = getStringValue("documentContent", msgNode);
            String requirement = getStringValue("requirement", msgNode);
            String workingTime = getStringValue("workingTime", msgNode);
            Integer bidCpc = getIntegerValue("bidCpc", msgNode);
            Integer contractCpc = getIntegerValue("contractCpc", msgNode);
            Double finalScore = getDoubleValue("finalScore", msgNode);
            Integer distributionOrder = getIntegerValue("distributionOrder", msgNode);
            Double originalScore = getDoubleValue("originalScore", msgNode);
            String systemId = getStringValue("systemId", msgNode);
            String updateDate = getStringValue("updateDate", msgNode);
            String jobVersionDate = getStringValue("jobVersionDate", msgNode);
            String trackingParam = getStringValue("trackingParam", msgNode);
            String removeParam = getStringValue("removeParam", msgNode);
            String advertiserId = getStringValue("advertiserId", msgNode);
            String campaignId = getStringValue("campaignId", msgNode);
            String campaignType = getStringValue("campaignType", msgNode);
            Integer maxCpc = getIntegerValue("maxCpc", msgNode);
            String jobChannelType = getStringValue("jobChannelType", msgNode);
            String yahooCompanyId = getStringValue("yahooCompanyId", msgNode);
            String distanceLat = getStringValue("distanceLat", msgNode);
            String distanceLon = getStringValue("distanceLon", msgNode);
            String algorithm = getStringValue("algorithm", msgNode);
            Double predictCtr = getDoubleValue("predictCtr", msgNode);
            Boolean isBackfill = getBooleanValue("isBackfill", msgNode);
            Integer totalHits = getIntegerValue("totalHits", msgNode);
            Integer addressLevel = getIntegerValue("addressLevel", AddressNode);
            String addressLevel1 = getStringValue("level1", AddressNode);
            String addressLevel2 = getStringValue("level2", AddressNode);
            String requestDateTime = getStringValue("requestDateTime", msgNode);
            Integer limit = getIntegerValue("limit", requestConditionNode);
            String keyword = getStringValue("keyword", requestConditionNode);
            Integer offset = getIntegerValue("offset", requestConditionNode);
            String location = getStringValue("location", requestConditionNode);
            String sort = getStringValue("keyword", requestConditionNode);
            String prefectureCode = getPrefectureCode("prefectureCode", msgNode);
            String mode = getStringValue("mode", msgNode);
            String notKeyword = getStringValue("notKeyword", msgNode);
            String companyName = getStringValue("companyName", requestConditionNode);
            String requestUrl = getStringValue("requestUrl", msgNode);

            String createDateTime = getCreateTime(node);

            return AdTracking
                    .newBuilder()
                    .setLogType(logType)
                    .setDistributionId(distributionId)
                    .setBucketType(distributionDateTime)
                    .setCategory(category)
                    .setRequestId(requestId)
                    .setDeviceType(deviceType)
                    .setIsRandom(isRandom)
                    .setCreateDateTime(createDateTime)
                    .setDocumentId(documentId)
                    .setPublisherId(publisherId)
                    .setIp(ip)
                    .setPublisherChannelId(publisherChannelId)
                    .setPublisherChannelType(publisherChannelType)
                    .setBucketType(bucketType)
                    .setDocumentUrl(documentUrl)
                    .setSiteCode(siteCode)
                    .setJobVersionDate(jobVersionDate)
                    .setDocumentTitle(documentTitle)
                    .setLogHost(log_host)
                    .setDocumentContent(documentContent)
                    .setRequirement(requirement)
                    .setWorkingTime(workingTime)
                    .setBidCpc(bidCpc)
                    .setContractCpc(contractCpc)
                    .setRole(role)
                    .setFinalScore(finalScore)
                    .setDistributionOrder(distributionOrder)
                    .setOriginalScore(originalScore)
                    .setSystemId(systemId)
                    .setTrackingParam(trackingParam)
                    .setTag(tag)
                    .setRemoveParam(removeParam)
                    .setAdvertiserId(advertiserId)
                    .setUaValue(uaValue)
                    .setUid(uid)
                    .setUpdateDate(updateDate)
                    .setVisitId(visitId)
                    .setCampaignId(campaignId)
                    .setCampaignType(campaignType)
                    .setMaxCpc(maxCpc)
                    .setJobChannelType(jobChannelType)
                    .setYahooCompanyId(yahooCompanyId)
                    .setLocation(location)
                    .setDistanceLat(distanceLat)
                    .setDistanceLon(distanceLon)
                    .setAlgorithm(algorithm)
                    .setPredictCtr(predictCtr)
                    .setMaxCpc(maxCpc)
                    .setIsBackfill(isBackfill)
                    .setTotalHits(totalHits)
                    .setAddressLevel(addressLevel)
                    .setAddressLevel1(addressLevel1)
                    .setAddressLevel2(addressLevel2)
                    .setRequestDateTime(requestDateTime)
                    .setLimit(limit)
                    .setKeyword(keyword)
                    .setOffset(offset)
                    .setSort(sort)
                    .setPrefectureCode(prefectureCode)
                    .setMode(mode)
                    .setNotKeyword(notKeyword)
                    .setCompanyName(companyName)
                    .setRequestUrl(requestUrl)
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);
            return null;
        }
    }
}
