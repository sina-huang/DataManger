import json
import hashlib
import redis
import websocket
import time
import requests
import queue
import threading
from settings import REDIS,OPENROUTER_API_KEY,URL,GPTDESC
from dataDeduplication import DataDeduplication
from dataAlignment import DataAlignment
from aggregator import OddsAggregator

"""
WebSocketProcessControl 类: 用于调度和控制的类。
它会开启三个线程，分别用于接收数据、处理数据和发送数据。
接收者线程：webscoket（123） ---->  队列（input_queue）
处理者线程：队列（input_queue）---->  队列（processed_queue）
发送者线程：队列（processed_queue）---->  webscoket（456）
"""

class WebSocketProcessControl:
    def __init__(self, receive_url, sender_url, GPTDESC, OPENROUTER_API_KEY,):
        self.receive_url = receive_url
        self.send_url = sender_url
        self.input_queue = queue.Queue()
        self.processed_queue = queue.Queue()
        self.r = redis.Redis(host=REDIS["host"], port=REDIS["port"], db=0,decode_responses=True)
        self.dupdata_num = 0
        self.newdata_num = 0
        self.error_num = 0
        self.gptask = 0
        self.temp_list = []
        self.temp_list_gpt =[]
        self.GPTDESC = GPTDESC
        self.OPENROUTER_API_KEY = OPENROUTER_API_KEY
        self.aggregator_all_odds_by_platform = {}
        self.aggregated_max_odds = {}


        # Threads for receiver, processor, and sender
        self.setup_receive_websocket()
        self.setup_processor()
        self.setup_send_websocket()

    def setup_receive_websocket(self):
        """Setup and start receiver WebSocket."""
        self.receiver_ws = websocket.WebSocketApp(self.receive_url,
                                                  on_open=self.on_receiver_open,
                                                  on_message=self.on_receiver_message,
                                                  on_error=self.on_receiver_error,
                                                  on_close=self.on_receiver_close)
        self.receiver_thread = threading.Thread(target=lambda: self.receiver_ws.run_forever())
        self.receiver_thread.daemon = True
        self.receiver_thread.start()

    def on_receiver_message(self, ws, message):
        """Handle incoming messages by placing them in the input queue."""
        message_json = json.loads(message)
        # print("接受者进程，接收到ws数据如下:", message_json)
        self.input_queue.put(message)

    def on_receiver_open(self, ws):
        print("Receiver WebSocket connected.")

    def on_receiver_error(self, ws, error):
        print("Receiver WebSocket error:", error)
        self.setup_receive_websocket()

    def on_receiver_close(self, ws, close_status_code, close_msg):
        print("Receiver WebSocket closed. Attempting to reconnect...")
        self.setup_receive_websocket()

    def setup_send_websocket(self):
        """Setup and start sender WebSocket."""
        self.sender_ws = websocket.WebSocketApp(self.send_url,
                                                on_open=self.on_sender_open,
                                                on_error=self.on_sender_error,
                                                on_close=self.on_sender_close)
        self.sender_thread = threading.Thread(target=lambda: self.sender_ws.run_forever())
        self.sender_thread.daemon = True
        self.sender_thread.start()

    def run_sender(self):
        """Process outgoing messages."""
        while True:
            data = self.processed_queue.get()
            # print("从processed队列中获取数据:", data)
            if data and self.sender_ws.sock and self.sender_ws.sock.connected:
                self.sender_ws.send(data)
                self.processed_queue.task_done()

    def on_sender_open(self, ws):
        print("Sender WebSocket connected.")

    def on_sender_error(self, ws, error):
        print("Sender WebSocket error:", error)
        self.setup_send_websocket()

    def on_sender_close(self, ws, close_status_code, close_msg):
        print("Sender WebSocket closed. Attempting to reconnect...")
        self.setup_send_websocket()

    def setup_processor(self):
        """Setup the data processing thread."""
        self.processor_thread = threading.Thread(target=self.process_data)
        self.processor_thread.daemon = True
        self.processor_thread.start()

    def process_data(self):
        """
        三步：1.数据去重，2.数据对齐，3.数据聚合
        去重：返回字符串，”new data“ or ”old data“ ,新数据需要后续处理，老数据直接丢弃
        对齐：返回字典，包含对齐结果
        聚合：返回字典，包含聚合结果
        :return:
        """
        while True:
            message = self.input_queue.get()
            # print("从input队列中获取数据:", message)
            dedup = DataDeduplication(message,self.r)
            dedup_check_result = dedup.run()
            # print("处理者进程，从input队列中拿到数据如下:", check_result)

            if dedup_check_result == "new data":
                # 去重检查通过，则进行数据对齐
                self.newdata_num += 1
                message_dict = json.loads(message)
                message_data_dict = json.loads(message_dict["message"])

                align_obj = DataAlignment(r=self.r,
                                          standardName_list=self.temp_list,
                                          GPTDESC=self.GPTDESC,
                                          gptask=self.gptask,
                                          OPENROUTER_API_KEY=self.OPENROUTER_API_KEY,
                                          data=message_data_dict)
                align_result_dict,self.gptask = align_obj.alignment_new_data()
                if "standardName" in align_result_dict:
                    aggregator = OddsAggregator(aggregator_all_odds_by_platform=self.aggregator_all_odds_by_platform,
                                                aggregated_max_odds=self.aggregated_max_odds)
                    # total_dict,max_odds_dict,bingo = aggregator.process_data(align_result_dict)
                    aggregator_dict, max_odds_dict, time_differences = aggregator.process_data(align_result_dict)
                    # print("对齐结果为:", total_dict)
                    print("aggregator:",aggregator_dict)
                    print("max_odds_dict:",max_odds_dict)
                    print("time_differences:",time_differences)


                    put_data_dict = {
                        "message":message_data_dict,
                        "total_data" : aggregator_dict,
                        "max_odds" : max_odds_dict,
                        "bingo":time_differences,
                        "dupdata_num": self.dupdata_num,
                        "newdata_num": self.newdata_num,
                        "error_num": self.error_num,
                        "gptask": self.gptask,
                        "input_queue":self.input_queue.qsize(),
                        "processed_queue":self.processed_queue.qsize(),

                    }
                    put_data_string = json.dumps(put_data_dict)
                    self.processed_queue.put(put_data_string)
            elif dedup_check_result == "duplicate data":
                self.dupdata_num += 1
            else:
                self.error_num += 1
                print("判定结果为格式错误数据，错误数量为:", self.dupdata_num)
            self.input_queue.task_done()


if __name__ == "__main__":
    controller = WebSocketProcessControl(receive_url=URL["receiverURL"],
                                         sender_url=URL["senderURL"],
                                         GPTDESC=GPTDESC,
                                         OPENROUTER_API_KEY=OPENROUTER_API_KEY
                                         )
    controller.run_sender()  # This will continue running due to while loop in run_sender method.
