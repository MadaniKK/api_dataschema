package com.mapxus.signalmapservice.api.v1;

import com.mapxus.signalmapservice.service.v1.TraceIdGeoJsonServiceV1;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/geojson")
public class TraceIdGeoJsonControllerV1 {
    private final TraceIdGeoJsonServiceV1 geoJsonServiceV1;

    public TraceIdGeoJsonControllerV1(TraceIdGeoJsonServiceV1 geoJsonServiceV1) {
        this.geoJsonServiceV1 = geoJsonServiceV1;
    }


//    @GetMapping("/byTraceIds")
//    public ResponseEntity getGeoJsonDataByTraceIds(@RequestParam(value = "traceIds") List<String> traceIds) {
//        /**
//         Get GeoJson Data by TraceIds
//         @Parameters:
//         - traceIds: a list of traceIds
//         @Returns:
//         responseEntity; Body: a list of geoJson
//        */
//        ResponseEntity responseEntity = geoJsonServiceV1.getGeoJsonDataByTraceIds(traceIds);
//
//        return responseEntity;
//    }
//
//    @GetMapping("/byTraceIdsAndTimeRange")
//    public ResponseEntity getGeoJsonDataWithinTimeRange(@RequestParam("traceIds") List<String> traceIds,
//                                                        @RequestParam("startTime") Long startTime,
//                                                        @RequestParam("endTime") Long endTime) {
//        /**
//         Get GeoJson Data by TraceIds Within TimeRange, returning trace whose start_timestamp within the time range
//         @Parameters:
//         - traceIds: a list of traceIds
//         - timeRange: [startTime, endTime], time are Strings in ISO 8601 format
//         @Returns:
//         responseEntity; Body: a list of geoJson
//        */
//
//        ResponseEntity responseEntity = geoJsonServiceV1.getGeoJsonDataWithinTimeRange(traceIds, startTime, endTime);
//
//        return responseEntity;
//    }

//    @GetMapping("/byTraceIdsAndBbox")
//    public ResponseEntity getGeoJsonDataWithinBbox(@RequestParam("traceIds") List<String> traceIds,
//                                                   @RequestParam("minLon") float minLon,
//                                                   @RequestParam("minLat") float minLat,
//                                                   @RequestParam("maxLon") float maxLon,
//                                                   @RequestParam("maxLat") float maxLat) {
//        /**
//         get GeoJson Data by TraceIds Within bbox
//         @Parameters:
//         - traceIds: a list of traceIds
//         - minLon
//         - minLat
//         - maxLon
//         - maxLat
//         @Returns:
//         responseEntity; Body: a list of geoJson
//        */
//
//        ResponseEntity responseEntity = geoJsonServiceV1.getGeoJsonDataWithinBbox(traceIds ,minLon, minLat, maxLon, maxLat);
//
//        return responseEntity;
//    }
    @GetMapping("/getGeoJson")
    public ResponseEntity getGeoJsonData(@RequestParam(value = "traceIds") List<String> traceIds,
                                          @RequestParam(value = "startTime", required = false) Long startTime,
                                          @RequestParam(value = "endTime", required = false) Long endTime,
                                          @RequestParam(value = "minLon", required = false) Float minLon,
                                          @RequestParam(value = "minLat", required = false) Float minLat,
                                          @RequestParam(value = "maxLon", required = false) Float maxLon,
                                          @RequestParam(value = "maxLat", required = false) Float maxLat) {
        /**
         Get GeoJson Data by TraceIds either based on startTime, EndTime or BBox
         When time param is Null, it is given a default time
         @Parameters:
         - traceIds
         - startTime
         - endTime
         - minLon
         - minLat
         - maxLon
         - maxLat
         @Returns:
         responseEntity: Body is a list of geoJsonData [{geoJson, properties:{traceId, startTime, EndTime}},...,{geoJson, properties:{traceId, startTime, EndTime}}]
         */

        ResponseEntity responseEntity;
        if (minLon != null && minLat != null && maxLon != null && maxLat != null){
            responseEntity = geoJsonServiceV1.getGeoJsonDataWithinBbox(traceIds, minLon, minLat, maxLon, maxLat, startTime, endTime);
        }else{
            responseEntity = geoJsonServiceV1.getGeoJsonDataWithinTimeRange(traceIds, startTime, endTime);
        }
        return responseEntity;


    }

    }
