from django.shortcuts import render
from django.http import HttpResponse
from rest_framework.decorators import api_view
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
import requests
from drf_yasg import openapi

from django.shortcuts import render
from django.http import HttpResponse
from rest_framework.decorators import api_view
from rest_framework import status
from kafka import KafkaProducer
import threading
import time
import json

#320 байт
# kafka-console-consumer --bootstrap-server localhost:29092 --topic Messages --from-beginning
# kafka-console-consumer --bootstrap-server localhost:29092 --delete --topic Messages
# kafka-topics --bootstrap-server localhost:29092 --delete --topic Messages

byte_size = 32

topic = "Messages"

producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        batch_size=1
    )

@swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
        'segment_number': openapi.Schema(type=openapi.TYPE_INTEGER, description='Номер сегмента'),
        'amount_segments': openapi.Schema(type=openapi.TYPE_INTEGER, description='Общее число сегментов'),
        'segment_data': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сегмента'),
        'dispatch_time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
        'sender': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
    }),
    operation_description="Положить сегмент в брокер сообщений Kafka"
)
@api_view(['POST'])
def post_segment(request):
    """
        Положить сегмент в брокер сообщений Kafka
    """
    producer.send(topic, request.data)

    return HttpResponse(status=200) 


@swagger_auto_schema(methods=['post'], request_body=openapi.Schema(type=openapi.TYPE_OBJECT, properties={
        'text': openapi.Schema(type=openapi.TYPE_STRING, description='Тело сообщения'),
        'time': openapi.Schema(type=openapi.TYPE_INTEGER, description='Время отправки сообщения'),
        'sender': openapi.Schema(type=openapi.TYPE_STRING, description='Отправитель'),
    }),
    operation_description="Разбить сообщение на сегменты длинной 320 байт и последовательная передача их на канальный уровень"
)
@api_view(['POST'])
def transfer_msg(request):
    """
        Разбить сообщение на сегменты длинной 320 байт и последовательная передача их на канальный уровень 
    """
    
    message_bytes = request.data['text'].encode('utf-8')

    segments = [message_bytes[i:i+byte_size] for i in range(0, len(message_bytes), byte_size)]

    for index, segment in enumerate(segments):
        segment_data = {'segment_number': len(segments) - 1 - index, 'amount_segments': len(segments),'segment_data': segment.decode('utf-8'), 'dispatch_time': request.data['time'], 'sender': request.data['sender']} # dispatch_time = time, sender - отправитель строка
        print(requests.post('http://172.20.10.4:8080/link_layer/', json=segment_data))
        # if (len(segments) - 1 - index == 0):
        #     break
        # producer.send(topic, segment_data)
        time.sleep(0.3)

    return HttpResponse(status=200)

def send_mesg_to_app_layer(time, sender, text, isError):

    json_data = {
        "time": time,
        "sender": sender,
        "text": text,
        "isError": isError
    } 
    requests.post('http://172.20.10.5:3000/recieve/', json=json_data)

    return 0

def read_messages_from_kafka(consumer):
    message_recieved = []
    while True:
        for message in consumer:
            message_str = message.value
            if (not len(message_recieved) or message_recieved[-1]['dispatch_time'] == message_str['dispatch_time']):
                message_recieved.append(message_str)
                if (message_str['segment_number'] == 0):
                    if (message_str['amount_segments'] == len(message_recieved)):
                        sorted_message = sorted(message_recieved, key=lambda x: x['segment_number'], reverse=True)
                        msg = ""
                        for i in range(len(sorted_message)):
                            msg += sorted_message[i]['segment_data']

                        send_mesg_to_app_layer(message_str['dispatch_time'], message_str['sender'], msg, 0)
                    else:
                        send_mesg_to_app_layer(message_str['dispatch_time'], message_str['sender'], "Error", 1)
                    message_recieved = []
            else:
                send_mesg_to_app_layer(message_recieved[-1]['dispatch_time'], message_recieved[-1]['sender'], "Error", 1)
                message_recieved = []
                message_recieved.append(message_str)
                print("Lost segment")
            

consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:29092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='test',
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

consumer_thread = threading.Thread(target=read_messages_from_kafka, args=(consumer,))

consumer_thread.start()


# # Функция для проверки и сборки сообщения из фрагментов
# def assemble_message_from_segments(consumer, topic):
#     for message in consumer:
#         segment_number = message.value['segment_number']
#         amount_segments = message.value['amount_segments']
#         sender = message.value['sender']
#         key = message.value['dispatch_time']

#         # Собираем фрагменты сообщения
#         try:
#             message_segments[key]
#         except:
#             message_segments[key] = list()
#         message_segments[key] = message.value

#         # Проверяем, все ли фрагменты собраны
#         if len(message_segments) == amount_segments:
#             # Сортируем фрагменты по номеру и собираем сообщение
#             # Сортируем ключи словаря в обратном порядке
#             sorted_keys = sorted(message_segments.keys(), reverse=True)

#             # Сортируем фрагменты по ключу и собираем сообщение
#             assembled_message = ''.join(message_segments[key] for key in sorted_keys)

#             data = {
#                 'sender': sender,
#                 'text': assembled_message,
#                 'time': key,
#                 'isError': False
#             }

#             requests.post('http://172.20.10.5/recieve/', data)
#             print(f"Сообщение: {assembled_message}")
#             # Здесь можно добавить логику удаления сообщений из очереди, если это необходимо
#             consumer.commit()
#     consumer.close()
#     return