__author__ = 'feeder'

import pymqi
import CMQC
from MQMessage import MQMessage

class MQConnector:

    def __init__(self, queue_manager, channel, host, port, *args):
        self.queue_manager_name = queue_manager
        self.channel_name = channel
        self.queues = {}
        self.queues_list = []
        self.message_data = {}
        self.conn_info = "%s(%s)" % (host, port)
        for queue in args:
            self.queues_list.append(queue)

    def _add_queue(self, queue_name):
        queue = pymqi.Queue(self.manager, queue_name)
        self.queues[queue_name] = queue

    def connect(self):
        self.manager = pymqi.connect(self.queue_manager_name, self.channel_name, self.conn_info)
        for queue in self.queues_list:
            self._add_queue(queue)

    def send_message(self, queue_name, message):
        self.queues[queue_name].put(message.message_string)

    def receive_message(self, queue_name):
        message = MQMessage()
        message.message_string = self.queues[queue_name].get(None, message.message_descriptor, message.gmo)
        message.reset()
        return message

    def _close_queue(self, queue):
        try:
            queue.close()
        except pymqi.Error:
            pass


    def disconnect(self):
        if (self.manager):
            self.manager.disconnect()


    def close_all_queues(self):
        for queue_name, queue in self.queues.items():
            self._close_queue(queue)







