import os

from tools.http_request_util import get_response_json




class DataSchemaServiceAPI:
    def __init__(self):
        self.token = os.getenv('POSTGREST_TOKEN')
        self.url = os.getenv('POSTGREST_URL')

    def query_wifi_data_by_trace_ids(self, floor_id, trace_ids, namespace):
        trace_id_list = ",".join(trace_ids)
        url = f"{self.url}/sensor_ap?select=spec,start_timestamp,trace_id&trace_id=in.({trace_id_list})&namespace=eq.{namespace}&labels->>floor=eq.{floor_id}"
        data = get_response_json(url, self.token)
        return data

    def query_groundTruth_by_trace_ids(self, floor_id, trace_ids, namespace):
        trace_id_list = ",".join(trace_ids)
        url = f"{self.url}/sensor_wifi?select=spec->groundTruth&trace_id=in.({trace_id_list})&namespace=eq.{namespace}&labels->>floor=eq.{floor_id}"
        data = get_response_json(url, self.token)
        return data

    def query_waypoints_for_track_df_by_trace_ids(self, floor_id, trace_ids, namespace):
        trace_id_list = ",".join(trace_ids)
        url = f"{self.url}/geo_proximity?select=spec->values,trace_id&trace_id=in.({trace_id_list})&namespace=eq.{namespace}&labels->>floor=eq.{floor_id}"
        data = get_response_json(url, self.token)
        return data

    def query_track_data_for_track_df_by_trace_ids(self, floor_id, trace_ids, namespace):
        trace_id_list = ",".join(trace_ids)
        url = f"{self.url}/sensor_step_count?select=spec->groundTruth,start_timestamp,trace_id&order=start_timestamp&trace_id=in.({trace_id_list})&namespace=eq.{namespace}&labels->>floor=eq.{floor_id}"
        data = get_response_json(url, self.token)
        return data

    def query_unique_trace_ids(self, floor_id, number):
        url = f"{self.url}/cooked_trace?select=trace_id&labels->>floor=eq.{floor_id}&order=start_timestamp&limit={number}"
        data = get_response_json(url, self.token)
        trace_ids = []
        for item in data:
            trace_ids.append(item['trace_id'])
        trace_ids = list(set(trace_ids))
        trace_ids = trace_ids[:number]
        return trace_ids

    # query by single trace_ids

    def query_ground_truth_by_trace_id(self, trace_id):
        url = f"{self.url}/sensor_wifi?select=spec&trace_id=eq.{trace_id}"
        data = get_response_json(url, self.token)
        return data

    def query_waypoints_data_for_track_df(self, trace_id):
        url = f"{self.url}/geo_proximity?select=spec->values&trace_id=eq.{trace_id}"
        data = get_response_json(url, self.token)
        return data

    def query_wifi_data_for_reading_df(self, trace_id):
        url = f"{self.url}/sensor_ap?select=spec,start_timestamp&trace_id=eq.{trace_id}"

        data = get_response_json(url, self.token)
        return data
