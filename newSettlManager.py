__author__ = 'feeder'

from MQConnector import MQConnector
from MQMessage import MQMessage
import pymqi
import CMQC

import os
import sys

queue_manager = "QM_giorgio"
channel = "S_giorgio"
host = "10.91.195.35"
port = "1414"
in_queue_name = "MTFASIAXTRM_Input"
out_queue_name = "MTFASIAXTRM_Output"

output_file = open('msgReceived.txt', 'a')
header_file = open('msgHeader.txt', 'a')
input_file = open('msgSending.txt', 'a')

#Messages flags and values:
susp_flag = 'N'

in_queue_name = "MTFASIAXTRM_Input"
out_queue_name = "MTFASIAXTRM_Output"

input_settl_file = open('msgSettlInfo.txt', 'r')
message_to_send_out_file = open('msgJuly02.txt', 'r')

settl_info = input_settl_file.readline()

connector = MQConnector(queue_manager, channel, host, port, in_queue_name, out_queue_name)

try:
    connector.connect()
except pymqi.MQMIError, e:
    connector.disconnect()

keep_running = True


while (keep_running):

    try:
        #Receive the message
        message_recvd = connector.receive_message(in_queue_name)

        input_msg_string = message_recvd.message_string

        print("Here's the message: ", input_msg_string)

        message_header = input_msg_string[:150]
        input_header = input_msg_string[:26] + susp_flag
        input_header += input_msg_string[27:110:]
        input_out_copy = input_msg_string[150:550:]
        header_file.write(message_header + os.linesep)
        message_string = input_header + input_out_copy + settl_info
        input_file.write(message_string + os.linesep)

        #Init Messages: output message
        output_message = MQMessage()
        output_message.set_defaults()

        #Writing the response message
        print("Writing the response: ", message_string)
        output_message.message_string = message_string
        connector.send_message(out_queue_name, output_message)

        print("Message SENT")

    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and e.reason == CMQC.MQRC_NO_MSG_AVAILABLE:
            # No messages, that's OK, we can ignore it.
            connector.close_all_queues()
            connector.disconnect()

            sys.exit()
        else:
            # Some other error condition.
            raise

