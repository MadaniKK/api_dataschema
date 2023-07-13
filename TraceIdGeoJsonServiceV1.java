package com.mapxus.signalmapservice.service.v1;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapxus.signalmapservice.feign.postgrest.PostgrestServiceApi;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.ZonedDateTime;





@Service
public class TraceIdGeoJsonServiceV1 {


    private  final RestTemplate restTemplate;

    private String postgrestUrl;

    private String postgrestToken;

    private PostgrestServiceApi postgrestServiceApi;

    @Autowired
    public TraceIdGeoJsonServiceV1(RestTemplate restTemplate,
                                   @Value("${postgrest.url}") String postgrestUrl,
                                   @Value("${postgrest.token}") String postgrestToken) {
        this.restTemplate = restTemplate;
        this.postgrestUrl = postgrestUrl;
        this.postgrestToken = postgrestToken;
    }


//    public ResponseEntity getGeoJsonDataWithinTimeRange(List<String> traceIds, Long startTime, Long endTime) {
//        String timeQuery = constructTimeQuery(startTime, endTime);
//        return executeGeoJsonRequest("/cooked_trace?select=geo,trace_id,start_timestamp,end_timestamp&order=start_timestamp.desc"  + "&trace_id=in.(" + String.join(",", traceIds) + ")" + timeQuery);
//    }

    public ResponseEntity getGeoJsonDataWithinTimeRange(List<String> traceIds, Long startTime, Long endTime) {
        String timeQuery = constructTimeQuery(startTime, endTime);
        String source = "cooked_trace";
        String param = "select=geo,trace_id,start_timestamp,end_timestamp&order=start_timestamp.desc"+ "&trace_id=in.(" + String.join(",", traceIds) + ")" + timeQuery;
        try{
            ResponseEntity<JsonNode> responseEntity = postgrestServiceApi.querySource(source,param);
            return processResponseEntity(responseEntity);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
    }




//    public ResponseEntity getGeoJsonDataWithinBbox(List<String> traceIds, Float minLon, Float minLat, Float maxLon, Float maxLat, Long startTime, Long endTime){
//        String timeQuery = constructTimeQuery(startTime, endTime);
//        String bboxQuery = "/rpc/query_cooked_trace_by_bbox?min_lon="+minLon+"&min_lat="+minLat+"&max_lon="+maxLon+"&max_lat="+maxLat+"&trace_ids=%7B"+String.join(",", traceIds)+ "%7D&order=start_timestamp.desc";
//        return executeGeoJsonRequest(bboxQuery+timeQuery);
//
//    }
    public ResponseEntity getGeoJsonDataWithinBbox(List<String> traceIds, Float minLon, Float minLat, Float maxLon, Float maxLat, Long startTime, Long endTime){
        String timeQuery = constructTimeQuery(startTime, endTime);
        String source = "query_cooked_trace_by_bbox";
        String param = "min_lon="+minLon+"&min_lat="+minLat+"&max_lon="+maxLon+"&max_lat="+maxLat+"&trace_ids=%7B"+String.join(",", traceIds)+ "%7D&order=start_timestamp.desc" + timeQuery;
        try{
            ResponseEntity<JsonNode> responseEntity = postgrestServiceApi.queryFuction(source, param);
            return processResponseEntity(responseEntity);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);

}

    private ResponseEntity processResponseEntity(ResponseEntity<JsonNode> responseEntity) throws Exception {
        JsonNode responseBody = responseEntity.getBody();
        if (responseBody == null) return ResponseEntity.ok(null);

        List<HashMap<String, Object>> geoData = processGeoJsonData(responseBody);
        if (geoData == null) {
            throw new Exception("Error in GeoJson Data deserialization");
        } else {
            return ResponseEntity.ok(geoData);
        }
    }

//    private ResponseEntity executeGeoJsonRequest(String query) {
//        try {
//            String apiUrl = postgrestUrl + query;
//            System.out.println(apiUrl);
//            HttpHeaders headers = new HttpHeaders();
//            headers.add("Authorization", "Bearer " + postgrestToken);
//            headers.setAccept(Arrays.asList(MediaType.ALL));
//
//            RequestEntity<Void> requestEntity = new RequestEntity<>(headers, HttpMethod.GET, new URI(apiUrl));
////            Long curTime = System.currentTimeMillis();
//            ResponseEntity<JsonNode> responseEntity = restTemplate.exchange(requestEntity, JsonNode.class);
////            Long curTime2 = System.currentTimeMillis();
////            Long timePassed = curTime2- curTime;
////            System.out.println("response time is: "+ timePassed +"ms");
//
//
//            if (responseEntity.getStatusCode().is2xxSuccessful()) {
//                JsonNode responseBody = responseEntity.getBody();
//                if (responseBody == null) return ResponseEntity.ok(null);
//                    List<HashMap<String, Object>> geoData = processGeoJsonData(responseBody);
//                    if (geoData == null) {
//                        throw new Exception("Error in GeoJson Data deserialization");
//                    } else {
//                        return ResponseEntity.ok(geoData);
//                    }
//            }
//        } catch (HttpClientErrorException e) {
//            if (e.getStatusCode() == HttpStatus.UNAUTHORIZED) {
//                System.out.println("Unauthorized request. Response Code: " + e.getStatusCode().value());
//                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
//            }
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
//    }

    private String constructTimeQuery(Long startTime, Long endTime) {
        StringBuilder queryBuilder = new StringBuilder();

        if (startTime != null) {
            String startTimeString = convertToUTC(startTime);
            queryBuilder.append("&start_timestamp=gte.").append(startTimeString);
        }
        if (endTime != null) {
            String endTimeString = convertToUTC(endTime);
            queryBuilder.append("&start_timestamp=lt.").append(endTimeString);
        }
        return queryBuilder.toString();
    }

    private List<HashMap<String, Object>> processGeoJsonData(JsonNode responseJsonNode) {
        List<HashMap<String, Object>> geoJsonList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            for (JsonNode featureNode : responseJsonNode) {
                JsonNode geoNode = featureNode.get("geo");
                HashMap<String, Object> geoJson = objectMapper.convertValue(geoNode, HashMap.class);
                String traceID = featureNode.get("trace_id").asText();
                String startTimeString = featureNode.get("start_timestamp").asText();
                Long startTimestamp = convertToUnixTimestamp(startTimeString);
                String endTimeString = featureNode.get("end_timestamp").asText();
                Long endTimestamp = convertToUnixTimestamp(endTimeString);

                HashMap<String, Object> properties = new HashMap<>();
                properties.put("trace_id", traceID);
                properties.put("start_timestamp",startTimestamp);
                properties.put("end_timestamp",endTimestamp);
                geoJson.put("properties", properties);
                geoJsonList.add(geoJson);
            }
            return geoJsonList;
        } catch (Exception e) {
            System.err.println("Error deserializing JSON: " + e.getMessage());
        }
        return null;
    }


    /**
     * Converts a millisecond Unix timestamp to an ISO 8601 formatted string.
     *
     * @param unixTimestampMillis The Unix timestamp in milliseconds.
     * @return The ISO 8601 formatted string representing the timestamp.
     */
    public static String convertToUTC(long unixTimestampMillis) {
        Instant instant = Instant.ofEpochMilli(unixTimestampMillis);
        return DateTimeFormatter.ISO_INSTANT.format(instant.atZone(ZoneId.of("UTC")));
    }

    /**
     * Converts an ISO 8601 formatted string to a millisecond Unix timestamp.
     *
     * @param timestamp ISO 8601 formatted string
     * @return The millisecond Unix timestamp.
     */
    public static long convertToUnixTimestamp(String timestamp) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        Instant instant = zonedDateTime.toInstant();
        return instant.toEpochMilli();
    }



}


