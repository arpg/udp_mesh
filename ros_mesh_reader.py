#!/usr/bin/python2
#ROS Mesh adapter
#the Reader reads from the mesh and produces local ROS messages
'''
from __future__ import division
from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from past.utils import old_div
from builtins import object
'''

from importlib import import_module
import rospy
import sys, logging, socket, os
from optparse import OptionParser


import StringIO
import io
import pdb
from udp_mesh.srv import *
from udp_mesh.msg import *

from enum import Enum
from Queue import Queue
import base64
from std_msgs.msg import *

#Given a MeshData message, publish a ROS message
#There's got to be a way to instantiate a publisher dynamically based on the type of the message we get from the mesh
#Note that this node has to be run from a workspace with knowledge of any message types that might be passed over the mesh
#Since said messages will have to be instantiated to be published

'''

from importlib import import_module

class Listener(object):
    def __init__(self):
        self._binary_sub = rospy.Subscriber(
            'some_topic', rospy.AnyMsg, self.topic_callback)

    def topic_callback(self, data):
        assert sys.version_info >= (2,7) #import_module's syntax needs 2.7
        connection_header =  data._connection_header['type'].split('/')
        ros_pkg = connection_header[0] + '.msg'
        msg_type = connection_header[1]
        print 'Message type detected as ' + msg_type
        msg_class = getattr(import_module(ros_pkg), msg_type)
        msg = msg_class().deserialize(self._buff)

        print data.known_field
'''

class MeshReader(object):
    def __init__(self):
        rospy.init_node('mesh_reader', anonymous=True)
        self.data_sub = rospy.Subscriber('inbound_data', PriorityMeshData, self.onDataPresent)
        self.topic_prefix = rospy.get_param('~topic_prefix', 'mesh_transport/')
        self.hostname =  rospy.get_param('~hostname')
        if len(self.hostname) == 0:
            self.hostname =  socket.gethostname()
        self.hosts = dict()
        
    def run(self):
        rospy.spin()

    def onDataPresent(self, msg):
        #Given a MeshData message, with a .data field containing an AnyMsg, decode to a typed topic
        #The MeshData envelope contains enough info to rehydrate the message
        #The way the envelope works, the .data field is the serialized ROS payload message

        host = msg.host #this is the host that sent the message
        topic = msg.topic
        #Payload message
        #Instantiate the correct type for the message

        typeFields = msg.type.split('/')
        ros_pkg = typeFields[0] + '.msg'
        msgType = typeFields[1]

        payloadClass = getattr(import_module(ros_pkg), msgType) #make sure we've got the message definition
        payloadMsg = payloadClass() #instantiate the class
        payloadMsg.deserialize(msg.data) #load er up

        #Make a publisher for this src and message if needed
        if not host in self.hosts:
            self.hosts[host] = dict()
        
        if not topic in self.hosts[host]:
            rospy.loginfo('Creating publisher for %s/%s' % (host, topic))
            fullTopic = '/%s/mesh_comm/%s/%s' % (host, self.hostname, topic)
            self.hosts[host][topic] = rospy.Publisher(fullTopic, payloadClass, queue_size=10)
            #
            #rospy.Publisher('%s/%s' % (host, topic), payloadClass, queue_size=10)

        
        self.hosts[host][topic].publish(payloadMsg)
        
if __name__ == '__main__':
    try:
        client = MeshReader()
        client.run()
    except rospy.ROSInterruptException:
        pass
