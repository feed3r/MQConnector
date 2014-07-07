__author__ = 'feeder'

import pymqi
import MQMessage

class mqConnector:
    def __init__(self, queue_manager, channel, host, port):
        self.queue_manager_name = queue_manager
        self.channel_name = channel
        self.host = host
        self.port = port
        self.queues = {}
        self.message_data = {}

        self.conn_info = "%s(%s)" % (host, port)

    def connect(self):
        self.manager = pymqi.connect(self.queue_manager, self.channel, self.conn_info)

    def set_queue(self, queue_name):
        queue = pymqi.Queue(self.manager_obj, queue_name)
        self.queue_dict[queue_name] = queue

    def get_queue(self, queue_name):
        return self.queue_dict[queue_name]

    def send_message(self, queue_name, message):
        self.queue_dict[queue_name].put(message.message_string)

    def receive_message(self, queue_name):
        message = MQMessage()
        message.message_string = self.queue_dict[queue_name].get(None, message.message_descriptor, message.message_gmo)
        message.reset()
        return message

    def close_queue(self, queue_name):
        queue = self.queue_dict[queue_name]
        queue.close()
        queue.disconnect()

    def close_all_queues(self):
        for queue in self.queue_dict:
            queue.close()
            queue.disconnect()






