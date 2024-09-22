import json
import hashlib
import time
import websocket
import threading
import redis
class DataDeduplication:
    def __init__(self, message, r):
        self.r = r
        self.message = message
        self.error_data = []

    def run(self):
        """
        分为两步：1.检查接收数据的结构，2.进行数据去重
        1. 检查是否为json格式，检查是否包含必须的字段，检查是否存在中文

        """
        check_errors_list,check_result_dict = self.check_data_structure(self.message)
        if check_errors_list:
            print(check_errors_list)
            return "error"
        else:
            return self.data_deduplication(check_result_dict)


    def data_deduplication(self, message):
        """
            :param message: 接收已经被拆包之后的数据，格式为json字典
            :return: False or True
            :desc: 1.使用redis中的list表用来去重，提取list最左边的数据与当前的最新数据进行比较，这里的比较仅仅比较哈希值.
                    (所以这里的去重，只不过是针对时间上的最近一条数据，也就是说，他其实是允许重复，但不允许和连续性的重复)
                   2.如果是重复数据，不添加，直接返回False
                   3.如果是新数据，添加到list表，并添加到hash表（存储数据，和时间戳），返回True
        """
        try:
            # 将数据转换为字符串,排序可以保证哈希值一致。

            message_string = json.dumps(message, sort_keys=True)
            message_sha256 = hashlib.sha256(message_string.encode('utf-8')).hexdigest()
            list_key = f"list:{message['Platform']}:{message['gameName']}"  # 设置redis键名
            zset_key = f"zset:{message['Platform']}:{message['gameName']}"  # 设置redis键名
            latest_item_hash = self.r.lindex(list_key, 0)  # 获取列表中最新的数据项（列表的最左边的第一个元素）
            if latest_item_hash and latest_item_hash == message_sha256:
                # 判断是否重复，如果重复，则返回False
                return "duplicate data"
            else:  # 不重复，则添加到list表，并添加到 zset ，返回True

                self.r.lpush(list_key, message_sha256)  # 添加到list表
                self.r.ltrim(list_key, 0, 999)  # 保持列表的长度为最多1000个元素
                current_time = time.time()
                self.r.zadd(zset_key, {message_string: current_time})  # 添加到zset表，并设置时间戳作为score,注意redis的键名。
                return "new data"
        except Exception as e:
            return f"error data_deduplication: {str(e)}"

    def check_data_structure(self, data):
        # 检查数据是否为字符串
        # if not isinstance(data, str):
        #     return "error: 数据不是字符串类型"

        # 检查是否为json结构的字符串
        message_content = None
        data_dict = None
        spider_data_dict = None
        try:
            if isinstance(data, dict):
                # print("收到字典一个")
                data_dict = data
            if isinstance(data,str):
                data_dict = json.loads(data)
        except:
            # print(type(data))
            self.error_data.append("check00: 数据不是字典类型或者json字符串")
            return "error: 数据不是字典类型"


        # 检查字典是否包含'message'键
        if 'message' not in data_dict:
            self.error_data.append("check01: 接收的数据中缺少 message 键")
        if 'message' in data_dict:
            try:
                message_content = json.loads(data_dict['message'])
            except json.JSONDecodeError:
                self.error_data.append("check02:接收的数据中有 message 键，但它的值不能转换成字典")
        if message_content:
            try:
                spider_data_dict = json.loads(data_dict['message'])
                required_keys = {'Platform', 'gameName', 'outcomes', 'teams', "leagueName"}
                if not all(key in spider_data_dict for key in required_keys):
                    self.error_data.append("check04: 缺少必要的键")
                self.check_chinese(spider_data_dict)

            except json.JSONDecodeError:
                self.error_data.append("check03: 接收的数据中有 message 键，但它的值不能转换成字典")

        return self.error_data,spider_data_dict

    def check_chinese(self, data):
        try:
            if isinstance(data, dict):
                for key, value in data.items():
                    # 检查键是否含有中文
                    if any('\u4e00' <= char <= '\u9fff' for char in key):
                        self.error_data.append(f"键中包含中文 - {key}")
                        return "error"
                    # 递归检查值
                    if isinstance(value, (dict, list)):
                        result = self.check_chinese(value)
                        if "error" in result:
                            return result
                    # 检查字符串类型的值是否含有中文
                    elif isinstance(value, str) and any('\u4e00' <= char <= '\u9fff' for char in value):
                        self.error_data.append(f"值中包含中文 - {value}")
                        return "error"
            elif isinstance(data, list):
                for item in data:
                    result = self.check_chinese(item)
                    if "error" in result:
                        return result
        except Exception as e:
            return f"error: 检查中文时发生错误 - {str(e)}"
        return "pass"


if __name__ == "__main__":
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.flushdb()
    print("redis清空成功")
    def on_message(ws, message):
        print("收到:",type(message), message)
        message = json.loads(message)
        deduplicator = DataDeduplication(message, redis_connection)  # 假设Redis连接为None
        result = deduplicator.run()
        print("处理结果:", result)


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



