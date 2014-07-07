__author__ = 'feeder'

import pymqi
import MQMessage

class mqConnector:

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
        self.manager = pymqi.connect(self.queue_manager, self.channel, self.conn_info)
        for queue in self.queues_list:
            self._add_queue(queue)

    def send_message(self, queue_name, message):
        self.queues[queue_name].put(message.message_string)

    def receive_message(self, queue_name):
        message = MQMessage()
        message.message_string = self.queue_dict[queue_name].get(None, message.message_descriptor, message.message_gmo)
        message.reset()
        return message

    def _close_queue(self, queue):
        queue.close()
        queue.disconnect()

    def close_all_queues(self):
        for queue_name, queue in self.queues.items():
            print("Closing Queue " + queue_name)
            self._close_queue(queue)







