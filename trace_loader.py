import time

import pandas as pd
# additional packages
from dateutil import parser
from tqdm.auto import tqdm

from api.data_schema_service import DataSchemaServiceAPI
from api.query_params import QueryingParams
from api.signal_map_service import SignalMapServiceAPI
from api.signal_mapping_positioning_analysis_service import SignalMappingPositioningAnalysisServiceAPI
from calculations.lat_lon_calculations import proximity_v5_parser, proximity_v4_parser, proximity_v3_parser, \
    cal_reading_latlon
from data_loader.enums.sensor_type import SensorType
from tools import fileUtils
from collections import defaultdict

# from tqdm import tqdm

tqdm.pandas()


def assemble_trace_path_df(floor_id, trace_ids, service, sensor_list=None, chunk_size=100):
    """
    assemble file path of proximity, step counter and required sensor data into a pd.DataFrame
    :param floor_id: floor id
    :param trace_ids: list
    :param sensor_list: sensor list, eg. [SensorType.Wifi, SensorType.Accelerometer]
    :param chunk_size: default 100, to avoid http query network error
    :return: pd.DataFrame
    index = trace id
    columns = ['StepCounter','Proximity','Wifi']
    column value is corresponding file path
    """
    if sensor_list:
        sensor_list = [s.name for s in sensor_list]
    p = QueryingParams()
    p["floorId"] = floor_id
    p["sensorType"] = [SensorType.StepCounter.name, *sensor_list]
    sensor_data_info = []
    proximity_info = []
    for i in tqdm(range(len(trace_ids) // chunk_size + 1), desc=f'Query data paths, chunk size={chunk_size}'):
        p["traceId"] = trace_ids[i * chunk_size:(i + 1) * chunk_size]
        sensor_data_info.extend(service.query_sensor_data(p))
        proximity_info.extend(service.query_proximities(p))

    path_df = pd.DataFrame(sensor_data_info, columns=['traceId', 'filePath', 'sensorType']).pivot(index='traceId',
                                                                                                  columns='sensorType')[
        'filePath']
    path_df[['Proximity', 'Proximity_version']] = \
        pd.DataFrame(proximity_info, columns=['traceId', 'filePath', 'proximityType', 'version']).set_index('traceId')[
            ['filePath', 'version']]
    if 'StepCounter' not in path_df.columns:
        path_df['StepCounter'] = pd.NA
    # print(path_df.head())
    return path_df


def parse_track(trace: pd.Series, container, source_type):
    proximity = fileUtils.load_data_by_lines(trace['Proximity'], container, source_type)
    if pd.notnull(trace['StepCounter']):
        step_count = fileUtils.load_data_by_lines(trace['StepCounter'], container, source_type)
    else:
        step_count = None

    if trace['Proximity_version'] == 5:
        timeline, track, waypoints = proximity_v5_parser(proximity, step_count)
    elif trace['Proximity_version'] == 4:
        timeline, track, waypoints = proximity_v4_parser(proximity)
    elif trace['Proximity_version'] == 3:
        timeline, track, waypoints = proximity_v3_parser(proximity)
    else:
        raise RuntimeError(
            f"""{trace.index}: proximity V{trace['Proximity_version']} file has no parser to execute!""")
    return timeline, track, waypoints


def wifi_txt_line_parser(txt_line):
    broken_line = txt_line.split(" ")
    if len(broken_line) >= 7:
        return int(broken_line[0]), ' '.join([str(s) for s in broken_line[1:-5]]), str(broken_line[-5]), int(
            broken_line[-4]), \
               int(broken_line[-3]), int(broken_line[-2]), int(broken_line[-1])
    else:
        return [pd.NA] * 7


def load_wifi(trace: pd.Series, container, source_type) -> pd.DataFrame:
    # blobstore might be accessed based on source_type
    lines = fileUtils.load_data_by_lines(trace['Wifi'], container, source_type)
    _tmp = []
    for line in lines:
        line = line.strip().decode()
        if line != '':
            _tmp.append(wifi_txt_line_parser(line))

    readings_df = pd.DataFrame(_tmp,
                               columns=['timestamp', 'ssid', 'bssid', 'rssi', 'fq', 'system_timestamp',
                                        'scan_timestamp'])
    readings_df['duplicated'] = readings_df.duplicated(subset=['bssid', 'system_timestamp'], keep=False)
    readings_df['trace_id'] = trace.name
    # print(readings_df.head())
    return readings_df


def get_wifi_reading_aps_and_gt(trace_ap_df: pd.DataFrame, track_series: pd.Series):
    timeline, latlons, _ = track_series.values
    grouped = trace_ap_df.groupby('timestamp')
    search_stone = 0
    gt_latlon_list = []
    reading_aps_dfs = []
    for timestamp in grouped.groups:
        _df = grouped.get_group(timestamp)
        if _df['duplicated'].sum() > 0:  # pass this reading if any ap reading within it is marked as duplicated
            continue
        lat, lon, search_stone = cal_reading_latlon(timestamp, timeline, latlons, search_stone)
        gt_latlon_list.append((lat, lon))
        reading_aps_dfs.append(_df[['bssid', 'rssi']].set_index('bssid').T)
    return reading_aps_dfs, gt_latlon_list


class Calculator:
    def __init__(self, timeline, latlons):
        self.search_stone = 0
        self.timeline = timeline
        self.latlons = latlons

    def cal(self, t: float):
        lat, lon, search_stone = cal_reading_latlon(t, self.timeline, self.latlons, self.search_stone)
        self.search_stone = search_stone
        return lat, lon


def get_ap_readings_and_gt(trace_ap_df: pd.DataFrame, track_series: pd.Series, trace_id):
    print("len of trace_ap_df 1: ", len(trace_ap_df))
    timeline, latlons, _ = track_series.values
    sorted_df = trace_ap_df['scan_timestamp'].sort_values()
    calculator = Calculator(timeline, latlons)
    trace_ap_df['gt'] = sorted_df.apply(calculator.cal)
    trace_ap_df['trace_id'] = trace_id
    return trace_ap_df


# ***************** use postgREST ******************
def get_unique_trace_ids(floor_id, number):
    pg = DataSchemaServiceAPI()
    trace_ids = pg.query_unique_trace_ids(floor_id, number)
    return trace_ids


def unixtimestamp_converter(timestamp_string):
    """
        Convert timestamps like "2022-03-10 04:25:57.05+00" to unix timestamps in milliseconds
        Args:
            timestamp_string: string
        Returns:
            type: int
    """
    timestamp = int(parser.parse(timestamp_string).timestamp() * 1000)
    return timestamp


def get_wifi_reading_aps(trace_ap_df: pd.DataFrame):
    """
            Get a list of ap_readings grouped by timestamp
            Args:
                trace_ap_df: pd.DataFrame, wifi_data of a trace_id stored in self.sensors_df_dict['Wifi']
            Returns:
                type: list
    """
    reading_aps_dfs = []
    grouped = trace_ap_df.groupby('timestamp')
    for timestamp in grouped.groups:
        _df = grouped.get_group(timestamp)
        if _df['duplicated'].sum() > 0:  # pass this reading if any ap reading within it is marked as duplicated
            continue
        reading_aps_dfs.append(_df[['bssid', 'rssi']].set_index('bssid').T)
    return reading_aps_dfs


def get_gt(floor_id, trace_ids, namespace):
    """
            Get a list of groundTruth of each group of ap_readings
            table: sensor_wifi
            column: spec
            Args:
                floor_id: floor_id
                trace_id: trace_id
            Returns:
                type: list
    """
    gt_list = []
    pg = DataSchemaServiceAPI()

    data = pg.query_groundTruth_by_trace_ids(floor_id, trace_ids, namespace)
    if data is not None:
        for _dict in data:
            [lon, lat] = _dict['groundTruth']
            gt_list.append((lat, lon))
        # print(gt_list)
    return gt_list


# ***************** use postgREST ******************


class TracePacket:
    def __init__(self, floor_id, trace_ids, sensor_list=None, data_service='sample', source='azure',
                 namespace='mapxustest-sms'):
        self.container = 'cloud-maphive-sensor-data'
        self.service = SignalMapServiceAPI()
        if data_service == 'analysis':
            self.container = 'cloud-maphive-smpas-sensor-data'
            self.service = SignalMappingPositioningAnalysisServiceAPI()
        elif data_service != 'sample':
            raise RuntimeError(f'data service must be either "sample" or "analysis", now: {data_service}')
        self.namespace = namespace
        self.source = source
        self.track_df = pd.DataFrame()
        self.sensors_df_dict = {}
        self.wifi_df = pd.DataFrame()
        self.floor_id = floor_id
        self.trace_ids = trace_ids
        self.sensor_list = sensor_list
        # self.path_df = assemble_trace_path_df(self.floor_id, self.trace_ids, self.service, sensor_list)
        # self.parse_tracks()
        self.construct_track_df()

    def get(self, sensor_name):
        return self.sensors_df_dict[sensor_name]

    def parse_tracks(self):
        print('\nParsing trace tracks: ')
        self.track_df = self.path_df.progress_apply(parse_track, args=(self.container, self.source), axis=1,
                                                    result_type='expand')
        self.track_df = self.track_df.rename(columns={0: 'time_line', 1: 'track', 2: 'waypoints'})
        return self.track_df

    def load_wifi_data(self):
        print('\nLoading Wifi raw txt: ')
        ## applies load_wifi to each column value, pulling the data based on the file path
        self.sensors_df_dict['Wifi'] = self.path_df.progress_apply(load_wifi, args=(self.container, self.source),
                                                                   axis=1)

    # ***************** use postgREST ******************

    def construct_track_df(self):
        df_data = {'trace_ids': [], 'time_line': [], 'track': [], 'waypoints': []}
        pg = DataSchemaServiceAPI()
        # contructing waypoints and time_line
        data = pg.query_waypoints_for_track_df_by_trace_ids(self.floor_id, self.trace_ids, 'mapxustest-sms')
        if data is not None:
            for trace_id_dict in data:
                trace_id = trace_id_dict['trace_id']
                points_dict = trace_id_dict['values']
                waypoints = []
                for point in points_dict:
                    lon = point['location'][0]
                    lat = point['location'][1]
                    waypoints.append((lat, lon))

                df_data['trace_ids'].append(trace_id)
                df_data['waypoints'].append(waypoints)

        track_data = pg.query_track_data_for_track_df_by_trace_ids(self.floor_id, self.trace_ids, 'mapxustest-sms')
        track_dict = defaultdict(list)
        timestamp_dict = defaultdict(list)
        if track_data is not None:
            for _dict in track_data:
                trace_id = _dict['trace_id']
                lon = _dict['groundTruth'][0]
                lat = _dict['groundTruth'][1]
                track_dict[trace_id].append((lat,lon))
                timestamp_dict[trace_id].append(unixtimestamp_converter(_dict['start_timestamp']))

        for trace_id in df_data['trace_ids']:
            df_data['track'].append(track_dict[trace_id])
            df_data['time_line'].append(timestamp_dict[trace_id])

        df_data = pd.DataFrame(df_data).set_index('trace_ids')
        df_data.columns = ['time_line', 'track', 'waypoints']
        self.track_df = df_data
        # print(self.track_df.head())

    def load_wifi_data_by_trace_id(self):
        pg = DataSchemaServiceAPI()
        start = time.time()
        data = pg.query_wifi_data_by_trace_ids(self.floor_id, self.trace_ids, 'mapxustest-sms')
        end = time.time()
        print(f"riley load wifi data time: {end - start}")

        if data is not None:
            readings_df = pd.json_normalize(data)
            # readings_df = pd.DataFrame(data)
            rename_dict = {
                'spec.rssi': 'rssi',
                'spec.ssid': 'ssid',
                'spec.wifiTimestamp': 'timestamp',
                'spec.frequency': 'fq',
                'spec.bssid': 'bssid',
                'spec.systemTimestamp': 'system_timestamp',
                'start_timestamp': 'scan_timestamp',
                'spec.groundTruth': 'gt',

            }

            readings_df = readings_df.rename(columns=rename_dict)
            readings_df[['timestamp', 'scan_timestamp']] = readings_df[['timestamp', 'scan_timestamp']].apply(
                lambda x: x.apply(unixtimestamp_converter))
            # readings_df = readings_df.sort_values('timestamp')
            # ******replace "" with 'unknown'****
            readings_df['ssid'] = readings_df['ssid'].replace('', 'unknown')
            # readings_df['trace_id'] = trace_id
            readings_df['duplicated'] = readings_df.duplicated(subset=['bssid', 'system_timestamp'], keep=False)

            # rearrange the orders of the columns
            desired_orders = ['timestamp', 'ssid', 'bssid', 'rssi', 'fq',
                              'system_timestamp', 'scan_timestamp', 'duplicated', 'trace_id', 'gt']
            readings_df = readings_df[desired_orders]
            self.wifi_df = readings_df

    def get_wifi_readings_and_gt2(self):
        if self.wifi_df.empty:
            start1 = time.time()
            self.load_wifi_data_by_trace_id()
            end1 = time.time()
        reading_aps_dfs = get_wifi_reading_aps(self.wifi_df)

        print(f"riley construct wifi readings time: {end1 - start1}")
        start2 = time.time()
        gt_latlon_list = get_gt(self.floor_id, self.trace_ids, 'mapxustest-sms')
        end2 = time.time()
        print(f"riley construct groundTruth time: {end2 - start2}")

        start3 = time.time()
        final_readings = pd.concat(reading_aps_dfs).reset_index(drop=True)
        end3 = time.time()
        print(f"riley concating readings time: {end3 - start3}")

        return final_readings, gt_latlon_list

    def get_ap_readings_with_gt2(self):
        # ap_readings with gt was already stored in self.wifi_df
        if self.wifi_df.empty:
            self.load_wifi_data_by_trace_id()

        return self.wifi_df

    # ***************** use postgREST ******************

    def get_wifi_readings_and_gt(self):
        if 'Wifi' not in self.sensors_df_dict.keys():
            self.load_wifi_data()
        wifi_dfs = self.sensors_df_dict['Wifi']
        reading_aps_dfs = []
        gt_latlon_list = []
        for trace_id in tqdm(wifi_dfs.index, desc=f'Parsing wifi reading ground truth of {len(wifi_dfs)} traces'):
            _reading_aps_dfs, _gt_latlon_list = get_wifi_reading_aps_and_gt(wifi_dfs.loc[trace_id],
                                                                            self.track_df.loc[trace_id])

            reading_aps_dfs.extend(_reading_aps_dfs)
            gt_latlon_list.extend(_gt_latlon_list)
        return pd.concat(reading_aps_dfs).reset_index(drop=True), gt_latlon_list

    def get_ap_readings_with_gt(self):
        if 'Wifi' not in self.sensors_df_dict.keys():
            self.load_wifi_data()
        wifi_dfs = self.sensors_df_dict['Wifi']
        ap_readings_dfs = []
        for trace_id in tqdm(wifi_dfs.index, desc=f'Parsing ap reading ground truth of {len(wifi_dfs)} traces'):
            _ap_readings_df = get_ap_readings_and_gt(wifi_dfs.loc[trace_id],
                                                     self.track_df.loc[trace_id], trace_id)
            ap_readings_dfs.append(_ap_readings_df)

        return pd.concat(ap_readings_dfs).reset_index(drop=True)


if __name__ == '__main__':
    # floor = "cda2a7ff225840988e461c1ceb34904b"
    # trace_ids = [

    #     # '654321_1686294939175'
    #     # '355841112969249_1680834855306',
    #     # '357537086675022_1685432235448', '222222222_1681963449378',
    #     # '111111666_1683276685989', '111111666_1683276703481', '111111666_1683276632360',
    #     # '222222222_1681963439267', '355841112969249_1684908298278',
    #     # trace_ids in test database:
    #     # '357513102358489_1647500978608', '357480102013581_1646886336475',
    #     "357536083376360_1672819332915",
    # ]
    # trace_ids = ['357481108223869_1664256961024', '357481108223869_1664240247366', '357481108223869_1664259517588',
    #              '357481108223869_1664255075562', '357481108223869_1664249542824', '357481108223869_1664249246020',
    #              '357481108223869_1664259592638', '357481108223869_1664245720450', '357481108223869_1664249024062',
    #              '357481108223869_1664158051551']
    floor = "bede5b4821ad4b72a0c67b24848bdfc2"
    pg = DataSchemaServiceAPI()
    trace_ids = get_unique_trace_ids(floor, 10)
    print(len(trace_ids))

    sensors = [SensorType.Wifi]
    start_time1 = time.time()
    traces = TracePacket(floor, trace_ids, sensors, data_service='sample', namespace='mapxustest-sms')
    end_time1 = time.time()
    time_track_df1 = end_time1 - start_time1
    print(f"riley tracePacket + track_df time: {time_track_df1}")

    start_time2 = time.time()
    wifi_readings_df, gt_list = traces.get_wifi_readings_and_gt2()
    end_time2 = time.time()
    time_track_df2 = end_time2 - start_time2
    print(f"riley wifi readings + groundTruth time: {time_track_df2}")
    # print(wifi_readings_df.head())
    start_time2 = time.time()
    ap_readings_df = traces.get_ap_readings_with_gt2()
    end_time2 = time.time()
    time_track_df2 = end_time2 - start_time2
    print(f"riley get_ap_readings_with_gt time: {time_track_df2}")
