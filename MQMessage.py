__author__ = 'feeder'

import CMQC
import pymqi

class MQMessage(object):
    def __init__(self):
        self.message_descriptor = pymqi.MD()
        self.message_string = ""
        self.reset()
        self.set_defaults()

    def reset(self):
        self.message_descriptor.MsgId = CMQC.MQMI_NONE
        self.message_descriptor.CorrelId = CMQC.MQCI_NONE
        self.message_descriptor.GroupId = CMQC.MQGI_NONE

    def set_options(self, options, wait_interval):
        self.gmo = pymqi.GMO
        self.gmo.Options = options
        self.gmo.WaitInterval = wait_interval

    def set_defaults(self):
        self.gmo = pymqi.GMO()
        self.gmo.Options = CMQC.MQGMO_WAIT | CMQC.MQGMO_FAIL_IF_QUIESCING
        self.gmo.WaitInterval = CMQC.MQWI_UNLIMITED # Unlimited expiration of queue