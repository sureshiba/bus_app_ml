import pika
import json
import pandas as pd
from threading import Lock
import os

# Создаем подключение к серверу RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Объявляем очереди
channel.queue_declare(queue='y_true')
channel.queue_declare(queue='y_pred')

# Создаем DataFrame для временного хранения данных
data = pd.DataFrame(columns=['y_true', 'y_pred'])
data.index.name = 'id'  # Устанавливаем имя индекса для удобства
lock = Lock()

# Функция для записи в файл
def write_to_csv(row):
    file_exists = os.path.isfile('./logs/metric_log.csv')
    with open('./logs/metric_log.csv', 'a') as f:
        if not file_exists:
            f.write('id;y_true;y_pred;absolute_error\n')
        f.write(f"{row['id']};{row['y_true']};{row['y_pred']};{row['absolute_error']}\n")

# Функция для обработки сообщений
def callback(ch, method, properties, body):
    with lock:
        message = json.loads(body)
        message_id = message.get('id')

        if message_id not in data.index:
            data.loc[message_id] = {'y_true': None, 'y_pred': None}

        if method.routing_key == 'y_true':
            message_body = message.get('body')
            data.loc[message_id, 'y_true'] = message_body
            print(f"Получено y_true: {message_body} с id: {message_id}")
        elif method.routing_key == 'y_pred':
            message_body = message.get('y_pred')
            data.loc[message_id, 'y_pred'] = message_body
            print(f"Получено y_pred: {message_body} с id: {message_id}")

        if pd.notna(data.loc[message_id, 'y_true']) and pd.notna(data.loc[message_id, 'y_pred']):
            y_true = data.loc[message_id, 'y_true']
            y_pred = data.loc[message_id, 'y_pred']
            absolute_error = abs(y_true - y_pred)

            write_to_csv({'id': message_id, 'y_true': y_true, 'y_pred': y_pred, 'absolute_error': absolute_error})
            print(f"Записана метрика для id: {message_id}")
            data.drop(message_id, inplace=True)

# Подписываемся на очереди
channel.basic_consume(queue='y_true', on_message_callback=callback, auto_ack=True)
channel.basic_consume(queue='y_pred', on_message_callback=callback, auto_ack=True)

# Запускаем режим ожидания сообщений
print('...Ожидание сообщений, для выхода нажмите CTRL+C')
channel.start_consuming()
