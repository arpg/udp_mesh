#!/usr/bin/python3

import sys, logging, os, socket, time

import threading
from threading import Condition, RLock

import fcntl
import struct
import logging
from errno import EINTR

from io import StringIO
import io
import pdb
import time

from enum import Enum
from queue import Queue
import base64
import struct
from MessageTypes import *
import rospy

def checksum256(st):
    return reduce(lambda x,y:x+y, map(ord, st)) % 256
   
class MessageFragment(object):
    FRAGMENT_HEADER_FORMAT = "<BHIIH"
    FRAGMENT_MAX_SIZE=1300
    
    #Given independent fields, construct a fragment

    #Given a binary message, unpack
    def __init__(self, msgType = None):
        self.msgType = msgType
        self.recvTime = 0.0
        self.lastXmitTime = 0.0
        self.priority = 0
        
    def decode(self, src):
        #Peek the first byte out of the src to determine type

        try:
            self.msgType = MessageTypes(struct.unpack("<B", src[0:1])[0])
        except ValueError:
            rospy.loginfo('MessageFragment.decode: Unknown message type %d' % struct.unpack("<B", src[0:1])[0])
            #Throw error, otherwise report up the call stack....
            raise Exception('Unknown message type %d' % struct.unpack("<B", bytes(src)[0:1])[0])
 
        if (self.msgType == MessageTypes.PAYLOAD) or (self.msgType == MessageTypes.PAYLOAD_NOACK):

            contents = struct.unpack("<HIIH", src[1:13])
            self.seq = contents[0]
            self.frag = contents[1]
            self.fragTotal = contents[2]
            self.data = src[13:-1] #leftovers, not including the checksum

            self.checksum = src[-1]

            #The profiler says the checksum is a slow point...
            #actualChecksum = checksum256(src[0:-1])
            #if self.checksum != actualChecksum :
            #    rospy.loginfo('MessageFragment.decode: Checksum failed! Wanted %d, got %d', self.checksum, actualChecksum)
                
        elif self.msgType == MessageTypes.ACK:
            contents = struct.unpack("<HI", src[1:7])
            self.seq = contents[0]
            self.frag = contents[1]
            
        elif self.msgType == MessageTypes.HOST:

            contents = struct.unpack('<HIIH', src[1:13])
            (hostname, ip)  = struct.unpack('<64p16p', src[13:-1])
            self.hostname = hostname
            self.ip = ip
          
        else:
            rospy.loginfo('MessageFragment.decode: Unknown message type %d' % self.msgType.value)
            

    def encodePayload(self, seq, frag, fragTotal, data, priority):
        self.seq = seq #(seq % (2^16 - 1))
        self.frag = frag
        self.fragTotal = fragTotal
        self.data = data
        self.priority = priority
        
    def encodeAck(self, seq, frag, fragTotal):
        self.seq = seq
        self.frag = frag
        self.fragTotal = fragTotal
        self.data = ''.encode()

    def encodeHost(self, hostname, ip):
        self.seq = 0
        self.frag = 0
        self.fragTotal = 0

        #Pascal strings as the underlying type
        self.data = struct.pack('<64p16p', hostname.encode(), ip.encode())
        
    def getBinary(self):
        #Return the packed binary representation of a fragment
        packer = struct.Struct(self.FRAGMENT_HEADER_FORMAT)

        #Use a BytesIO buffer so that I can compute the checksum over the front matter
        buf = io.BytesIO()
        writer = io.BufferedWriter(buf)
        writer.write(packer.pack(self.msgType.value, self.seq, self.frag, self.fragTotal, len(self.data)))
        writer.write(self.data)
        writer.flush()

        #Compute the checksum over the frontmatter and stash it as well
        #writer.write(struct.pack('<B', checksum256(buf.getvalue())))
        writer.write(struct.pack('<B', 0))

        writer.flush()
        #Return the finished product:
        return buf.getvalue()
        
        
