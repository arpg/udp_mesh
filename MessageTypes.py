#!/usr/bin/python2
from enum import Enum

class MessageTypes(Enum):
    PAYLOAD = 1
    ACK = 2
    HOST = 4
    PAYLOAD_NOACK = 5
