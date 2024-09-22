from datetime import datetime
from settings import *
import json
import websocket
import threading
import redis
from dataDeduplication import DataDeduplication
from dataAlignment import DataAlignment
class OddsAggregator:
    def __init__(self,aggregator_all_odds_by_platform,aggregated_max_odds):
        """
        :param aggregator_all_odds_by_platform: 汇总所有赔率和平台到一个dict里面
        :param aggregated_max_odss: 选最大赔率的数据
        """
        self.all_odds_by_platform = aggregator_all_odds_by_platform
        self.aggregated_max_odds = aggregated_max_odds
        self.bingo = []  # 用于存储每个标准名下最后一次倒数和小于1的时间
        self.time_differences = []  # 存储时间差和最大值 {standard_name: {'time_diff': [], 'max_value': []}}
        self.current_states = {}  # 当前状态 {standard_name: {'timestamp': datetime, 'max_value': float}}

    def process_data(self, data_dict):
        """
        :param data_dict: 接收对齐之后的数据，且确保有"standardName"项的数据
        :return: 汇总数据和最大赔率数据
        """
        standard_name = data_dict['standardName']
        outcomes = data_dict["outcomes"]
        # print("outcomes：",outcomes)
        if len(outcomes) == 3:
            try:
                # 确保outcomes至少有三个元素，并且每个元素都是字典且包含'value'键
                home_team_odds = float(list(outcomes[0].values())[0])
                draw_odds = float(list(outcomes[1].values())[0])
                away_team_odds =float( list(outcomes[2].values())[0])
            except IndexError:
                # 发生IndexError时，可能是因为outcomes中的元素少于三个
                home_team_odds, draw_odds, away_team_odds = 0, 0, 0  # 或者设置为默认值
        else:
            home_team_odds, draw_odds, away_team_odds = 0 , 0 , 0
        self.update_platform_odds(standard_name, data_dict["Platform"], home_team_odds, draw_odds, away_team_odds)
        self.update_aggregated_max_odds(standard_name)
        return self.all_odds_by_platform,self.aggregated_max_odds,self.time_differences




    def update_platform_odds(self, standard_name, platform, home_team_odds, draw_odds, away_team_odds):

        if standard_name not in self.all_odds_by_platform:
            # 构建一个空字典，方便后续添加数据
            self.all_odds_by_platform[standard_name] = {}
        # 更新最新赔率
        self.all_odds_by_platform[standard_name][platform] = {
            'home_odds': home_team_odds,
            'draw_odds': draw_odds,
            'away_odds': away_team_odds
        }

    def update_aggregated_max_odds(self,standard_name):
        if standard_name not in self.all_odds_by_platform:
            print("生成最大赔率时报错，很可能没有找到对应的standard_name")
            return

        # 获取特定比赛的所有平台赔率
        match_odds = self.all_odds_by_platform[standard_name]

        # 初始化最大赔率记录
        max_home_odds = {'odds': 0, 'from': None}
        max_draw_odds = {'odds': 0, 'from': None}
        max_away_odds = {'odds': 0, 'from': None}

        # 遍历所有平台的赔率，更新最大赔率
        for platform, odds in match_odds.items():
            if float(odds['home_odds']) >= float(max_home_odds['odds']):
                max_home_odds = {'odds': odds['home_odds'], 'from': platform}
            if float(odds['draw_odds']) >= float(max_draw_odds['odds']):
                max_draw_odds = {'odds': odds['draw_odds'], 'from': platform}
            if float(odds['away_odds']) >= float(max_away_odds['odds']):
                max_away_odds = {'odds': odds['away_odds'], 'from': platform}

        # 更新聚合数据
        self.aggregated_max_odds[standard_name] = {
            'home_max_odds': max_home_odds,
            'draw_max_odds': max_draw_odds,
            'away_max_odds': max_away_odds
        }
        # 安全地计算倒数和
        home_odds = float(max_home_odds['odds']) if max_home_odds['odds'] > 0 else float('inf')
        draw_odds = float(max_draw_odds['odds']) if max_draw_odds['odds'] > 0 else float('inf')
        away_odds = float(max_away_odds['odds']) if max_away_odds['odds'] > 0 else float('inf')

        inverse_sum = 1 / home_odds + 1 / draw_odds + 1 / away_odds
        # if inverse_sum < 1 :
        #     self.bingo.insert(0,{"standard_name":standard_name,"total_odds":inverse_sum})
        print("当前的赔率和值为：", inverse_sum)
        self.calculate_duration_below_one(standard_name, inverse_sum)

    def calculate_duration_below_one(self, standard_name, max_value):
        current_time = datetime.now()
        if max_value < 1 and max_value !=0:
            # 当最大值小于1时，记录状态
            self.current_states[standard_name] = {'timestamp': current_time, 'max_value': max_value}
            print("小于1的情况", self.current_states)
        elif standard_name in self.current_states and max_value > 1:

            # 提取并移除当前状态
            start_data = self.current_states.pop(standard_name)
            start_time = start_data['timestamp']
            start_max_value = start_data['max_value']
            # 计算时间差
            time_diff = (current_time - start_time).total_seconds()
            # 存储时间差和更新最大值
            self.time_differences.insert(0,{
                'standard_name': standard_name,
                'time_diff': time_diff,
                'start_max_value': start_max_value,
            })

if __name__ == '__main__':
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.flushdb()
    print("redis清空成功")
    stardand_list =[]


    def on_message(ws, message):
        # print("收到:", type(message), message)
        message_dict = json.loads(message)
        deduplicator = DataDeduplication(message_dict, redis_connection)  # 假设Redis连接为None
        dedup_check_result = deduplicator.run()
        if dedup_check_result == "new data":
            # 去重检查通过，则进行数据对齐
            print(type(message))
            # message_dict = json.loads(message)
            message_data_dict = json.loads(message_dict["message"])

            align_obj = DataAlignment(r=redis_connection,
                                      standardName_list=stardand_list,
                                      GPTDESC=GPTDESC,
                                      OPENROUTER_API_KEY=OPENROUTER_API_KEY,
                                      data=message_data_dict)
            align_result_dict = align_obj.alignment_new_data()
            if "standardName" in align_result_dict:

                total_dict, max_odds_dict, time_differences = aggregator.process_data(align_result_dict)
                print(total_dict)
                print(max_odds_dict)
                print(time_differences)




    def on_error(ws, error):
        print("Error  on_error:", error)


    def on_close(ws, close_status_code, close_msg):
        print("### closed ###", close_status_code, close_msg)


    def on_open(ws):
        print("Opened connection")

        def run(*args):
            # 发送消息到WebSocket服务器，如果需要的话
            # ws.send('{"hello": "world"}')
            pass

        threading.Thread(target=run).start()


    # websocket.enableTrace(True)
    global redis_connection
    global aggregator
    aggregator = OddsAggregator(aggregator_all_odds_by_platform={},
                                aggregated_max_odds={})

    redis_connection = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    ws = websocket.WebSocketApp("ws://192.166.82.38:8000/ws/some_path/",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()