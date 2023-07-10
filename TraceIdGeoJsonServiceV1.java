package com.mapxus.signalmapservice.service.v1;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.format.DateTimeFormatter;



@Service
public class TraceIdGeoJsonServiceV1 {


    private  final RestTemplate restTemplate;

    private String postgrestUrl;

    private String postgrestToken;

    @Autowired
    public TraceIdGeoJsonServiceV1(RestTemplate restTemplate,
                                   @Value("${spring.postgrest.url}") String postgrestUrl,
                                   @Value("${spring.postgrest.token}") String postgrestToken) {
        this.restTemplate = restTemplate;
        this.postgrestUrl = postgrestUrl;
        this.postgrestToken = postgrestToken;
    }

    public ResponseEntity getGeoJsonData(List<String> traceIds) {
        return executeGeoJsonRequest("/cooked_trace?select=geo,trace_id&trace_id=in.(" + String.join(",", traceIds) + ")");
    }

    public ResponseEntity getGeoJsonDataWithinTimeRange(List<String> traceIds, Long startTime, Long endTime) {
        String startTimeString = convertToISO8601(startTime);
        String endTimeString = convertToISO8601(endTime);
        String query = "&start_timestamp=gte." + startTimeString + "&start_timestamp=lt." + endTimeString + "&trace_id=in.(" + String.join(",", traceIds) + ")";
        return executeGeoJsonRequest("/cooked_trace?select=geo,trace_id" + query);
    }

    public ResponseEntity getGeoJsonDataWithinBbox(List<String> traceIds, Float minLon, Float minLat, Float maxLon, Float maxLat){
        return executeGeoJsonRequest("/rpc/bbox_query_cooked_trace?min_lon="+minLon+"&min_lat="+minLat+"&max_lon="+maxLon+"&max_lat="+maxLat+"&trace_ids=%7B"+String.join(",", traceIds)+"%7D");

    }

    private ResponseEntity executeGeoJsonRequest(String query) {
        try {
            String apiUrl = postgrestUrl + query;
            HttpHeaders headers = new HttpHeaders();
            headers.add("Authorization", "Bearer " + postgrestToken);
            headers.setAccept(Arrays.asList(MediaType.ALL));

            RequestEntity<Void> requestEntity = new RequestEntity<>(headers, HttpMethod.GET, new URI(apiUrl));
            ResponseEntity<JsonNode> responseEntity = restTemplate.exchange(requestEntity, JsonNode.class);

            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                JsonNode responseBody = responseEntity.getBody();
                List<HashMap<String, Object>> geoData = processGeoJsonData(responseBody);
                if (geoData == null) {
                    throw new Exception("Error in GeoJson Data deserialization");
                } else {
                    return ResponseEntity.ok(geoData);
                }
            }
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.UNAUTHORIZED) {
                System.out.println("Unauthorized request. Response Code: " + e.getStatusCode().value());
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
            }
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    private List<HashMap<String, Object>> processGeoJsonData(JsonNode responseJsonNode) {
        List<HashMap<String, Object>> geoJsonList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            for (JsonNode featureNode : responseJsonNode) {
                JsonNode geoNode = featureNode.get("geo");
                String traceID = featureNode.get("trace_id").toString();
                HashMap<String, Object> geoJson = objectMapper.convertValue(geoNode, HashMap.class);
                HashMap<String, Object> properties = new HashMap<>();
                properties.put("trace_id", traceID);
                geoJson.put("properties", properties);
                geoJsonList.add(geoJson);
            }
            return geoJsonList;
        } catch (Exception e) {
            System.err.println("Error deserializing JSON: " + e.getMessage());
        }
        return null;
    }



    public String convertToISO8601(long unixTimestampMillis) {
        /**
         * Converts a millisecond Unix timestamp to an ISO 8601 formatted string.
         *
         * @param unixTimestampMillis The Unix timestamp in milliseconds.
         * @return The ISO 8601 formatted string representing the timestamp.
         */
        Instant instant = Instant.ofEpochMilli(unixTimestampMillis);
        return DateTimeFormatter.ISO_INSTANT.format(instant);
    }


}


