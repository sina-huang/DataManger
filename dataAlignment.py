import json
import requests
from settings import *
import json
import websocket
import threading
import redis
from dataDeduplication import DataDeduplication

class DataAlignment:
    def __init__(self, r, standardName_list, GPTDESC,OPENROUTER_API_KEY,data,model=GPT4, gptask=0):
        """
        :param data : 比赛数据（去重且清洗之后的数据）
        :param r:  redis
        :param temp_list: 用于发送给GPT做匹配的列表
        :param GPTDESC:    GPT的描述
        :param gptask:     记录GPT请求次数
        :param OPENROUTER_API_KEY:   GPT的API_KEY
        :return:  给data配置好standardName字段，返回data  或者 返回 None
        """
        self.r = r
        self.standardName_list = standardName_list
        self.GPTDESC = GPTDESC
        self.gptask = gptask
        self.OPENROUTER_API_KEY = OPENROUTER_API_KEY
        self.data = data
        self.model = model

    def alignment_new_data(self):
        """
        :param data: 各个平台通过websocket发送过来的数据
        :return: 替换完名称之后的数据
        :Decs  : 1.根据数据中的platform和gameName，去redis中查找是否存在标准名称,有直接替换gameName,没有继续
                     2.去temp_list 中找，
                        2.1是否有对应的名称，有则直接替换gameName,同时添加到redis中
                        2.2 temp_list没有，则GPT请求，返回结果，分析
                            2.2.1 如果匹配成功，说明，只是名字不一样，但是比赛是同一场比赛。
                            所以，将名字存到redis中，不需要添加到temp_list中。下次直接就可以从redis中找到了。
                            2.2.2 如果匹配失败，说明，确实是一场新的比赛。
                            既要存在redis中，也需要存到temp_list中，方便下次匹配。
                所以，两个数据结构，第一redis是用于比对已经存在的比赛，第二temp_list是用于提交给GPT的。
        """
        # Redis 哈希表键(键名：hash:platform，域名：gameName，值：standardName)

        self.hash_key = f"hash:{self.data['gameName']}"
        try:
            check_redis = self.r.get(self.hash_key)  # 去找数据库中，是否存在对应的标准名称
        except Exception as e:
            return f"error: {str(e)}"

        if check_redis is not None:
            # 如果redis中存在，则替换platform_game_name为标准名称
            self.data["standardName"] = check_redis

        if check_redis is None:
            # redis中不存在，开始动态匹配
            if not self.standardName_list:
                # temp_list为空，执行
                self.when_temp_list_null()

            else:
                # temp_list不为空时，执行
                ask_gpt_name = self.data['gameName'].strip()+' -- '+self.data['leagueName'].strip()
                if ask_gpt_name not in self.standardName_list:
                    # temp_list不为空,且data['gameName']不在temp_list中
                    self.when_not_in_temp_list()
                    # print(self.standardName_list)
                if ask_gpt_name in self.standardName_list:
                    # temp_list不为空,但data['gameName']在temp_list中
                    self.data["standardName"] = self.data['gameName'].strip()

        return self.data,self.gptask






       

    def when_temp_list_null(self):
        """
        构造一个请求GPT时所需的标准比赛名字，并添加到temp_list中
        desc:   依次执行 加入temp_list，比赛名加入redis，设置新数据项standardName字段
        :return: 没有返回值，直接构造了
        """
        # 如果temp_list为空

        gptAskName = self.data['gameName'].strip()+' -- '+self.data['leagueName'].strip()
        self.standardName_list.insert(0,gptAskName)  # 入数列
        self.r.set(self.hash_key, self.data['gameName'])  # 入redis
        self.data["standardName"] = self.data['gameName']


    def when_not_in_temp_list(self):
        """
        已经判断过，第一新比赛名字不在redis里面，第二新比赛名字不在temp_list里面
        说明，确实是新的比赛名，而不是赔率的变化。所以，需要提交给GPT进行匹配
        desc:   依次执行
                1.gptRequest()
                2.check_respnose()
                3.gpt_response_process
        """

        platform_data = {
            "Platform": self.data['Platform'],
            "gameName": self.data['gameName'],
            "leagueName":self.data['leagueName'],
        }
        desc = self.GPTDESC.format(standard_list=json.dumps(self.standardName_list[-100:]),
                                   platform_data=json.dumps(platform_data)
                                   , ensure_ascii=False)
        # 发送请求给GPT
        response = self.gptRequest(desc)
        self.gptask += 1
        # 检查GPT返回的结构是否合规
        gpt_response_content_dict = self.check_response(response)
        if gpt_response_content_dict is not None:
            # GPT返回已合规，则开始处理返回的数据
            self.gpt_response_process(gpt_response_content_dict)

        if "standardName" not in self.data or gpt_response_content_dict is None:
            return None

    def gptRequest(self, desc):
        """
        :param desc:  GPT的描述
        :return:  GPT返回经过处理之后的dict数据 或者是 None
        """
        try:
            response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.OPENROUTER_API_KEY}",
                },
                data=json.dumps({
                    "model": self.model,  # Optional
                    "messages": [{"role": "user", "content": desc}],

                })
            )

        except requests.exceptions.RequestException as e:
            print("GPT请求发生网络问题", e)
            return None
        return response

    def gpt_response_process(self, gpt_response_content_dict):
        """
        :param gpt_response_content_dict: gpt请求的返回的数据部分
        :return: 没有返回，直接修改self.data
        """
        # 处理字典内容
        match_result = gpt_response_content_dict.get("matchResult", "")
        # print(match_result)
        if match_result == "matchSuccess":
            """
            GPT匹配成功，说明是同一场比赛，只是名称不一样
            所以不需要添加到standardName_list中，但是需要添加到redis中
            """
            standard_name = gpt_response_content_dict.get("successData", {}).get("matchName", "")
            original_name = gpt_response_content_dict.get("OriginalName", {})

            self.r.set(self.hash_key, standard_name)
            self.data["standardName"] = standard_name
            print(f"判定为同一场比赛----标准名：{standard_name},原名：{original_name}")
        else:
            """
            GPT匹配失败，说明是新的比赛
            所以需要添加到standardName_list中，也需要添加到redis中
            """
            gptAskName = self.data['gameName'].strip() + ' -- ' + self.data['leagueName'].strip()
            self.r.set(self.hash_key, self.data['gameName'])
            self.standardName_list.insert(0,gptAskName)
            self.data["standardName"] = self.data["gameName"]
            print(f"判定为新的比赛：{self.data['gameName']}")

    def check_response(self, response):
        """
        :param response: gpt请求的原始response
        :return: gpt返回经过处理之后的dict数据 或者是 None
        """

        if response.status_code == 200:
            try:
                # 直接解析JSON
                gpt_resp_dict = response.json()
                gpt_resp_choices_dict = gpt_resp_dict.get('choices', [{}])[0]
                gpt_resp_message_dict = gpt_resp_choices_dict.get('message', {})

                gpt_resp_content_str = gpt_resp_message_dict.get('content', '')
                # print(gpt_resp_content_str)
                gpt_resp_content_str = gpt_resp_content_str.replace('```json', '').replace('```', '').strip()

                if gpt_resp_content_str:
                    # gpt返回的内容数据不为空
                    try:
                        gpt_resp_content_dict = json.loads(gpt_resp_content_str)
                        if 'matchResult' not in gpt_resp_content_dict:
                           return None
                        return gpt_resp_content_dict
                    except json.JSONDecodeError:
                        print("JSON解析失败")
                        return None
                return None
            except requests.exceptions.JSONDecodeError:
                print("JSON解析失败，GPT返回数据格式不正确")
                return None
        else:
            print("网络问题GPT连接出问题，返回请求码为", response.status_code)
            print("GPT的返回结果为内容为:", response)
            return None

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
            # print(type(message))
            # message_dict = json.loads(message)
            message_data_dict = json.loads(message_dict["message"])

            align_obj = DataAlignment(r=redis_connection,
                                      standardName_list=stardand_list,
                                      GPTDESC=GPTDESC,
                                      OPENROUTER_API_KEY=OPENROUTER_API_KEY,
                                      data=message_data_dict)
            align_result_dict = align_obj.alignment_new_data()






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
    redis_connection = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    ws = websocket.WebSocketApp("ws://192.166.82.38:8000/ws/some_path/",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()