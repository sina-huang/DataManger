import websocket
import json
import time

def send_message(ws, message):
    try:
        json_data = json.dumps(message)
        ws.send(json_data)
        print("Sent:", json_data)
    except Exception as e:
        print("Failed to send message:", str(e))

def on_message(ws, message):
    print("Received message:", message)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws):
    print("Connection closed.")

def on_open(ws):
    print("Connection established, you can start sending messages.")
    with open("sendData.txt", "r", encoding='utf-8') as file:
        for line in file:
            print("等待用户按下回车...")
            input()  # 等待用户按下回车
            try:
                message = json.loads(line.strip()) # 假设每行都是一个有效的JSON字符串
                send_message(ws, message)
            except json.JSONDecodeError:
                print("Invalid JSON format in line:", line)
            except Exception as e:
                print("An error occurred:", str(e))
    ws.close()

if __name__ == "__main__":
    websocket_url = "ws://192.166.82.38:8000/ws/some_path/"  # 确保这是正确的URL

    while True:
        print("Attempting to connect...")
        ws = websocket.WebSocketApp(websocket_url,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    on_open=on_open)
        ws.run_forever()
        print("Trying to reconnect in 5 seconds...")
        time.sleep(1)  # 等待5秒后尝试重新连接
