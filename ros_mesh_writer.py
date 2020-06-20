#!/usr/bin/python2
#ROS Mesh adapter
#Listen on a topic - when we get a message, prepare a MeshData message for transmission

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
import sys, logging, os
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
from std_srvs.srv import *
'''


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
def makeUInt8(arg):
    return struct.pack("B", arg)

class MeshWriter(object):
    
    def __init__(self):
        rospy.init_node('mesh_writer', anonymous=True)
        self.data_pub = rospy.Publisher('outbound_data', PriorityMeshData, queue_size=10)
        self.hosts_sub = rospy.Subscriber('hosts', HostEntry, self.onHostEntry)
        self.topic_list = rospy.get_param('~topic_list', [])
        self.topic_prefix = rospy.get_param('~topic_prefix', 'mesh_transport')
        
        #rospy.loginfo('Got topic list:')
        #rospy.loginfo(self.topic_list)


        #Topics take the form of
        # '1:/camera/image_raw', '4:/camera_info'
        #
        # to send images at priority 1, and camera info at priority 4
        ##Note: the 'any' topic name is special - it will just spit out something named as the type provided
        self.hosts = dict()
        self.addTopicsForHost('') #indicating that this is a broadcast message to all hosts
        self.updateHosts()
        
    def run(self):
        rospy.spin()

    def onHostEntry(self, msg):
        if msg.host not in self.hosts:
            self.addTopicsForHost(msg.host)

    def addTopicsForHost(self, host, topicPrefix = 'mesh_comm'):
        self.hosts[host] = dict()
        rospy.loginfo('Creating subscribers for %s' % (host if host != '' else 'broadcast'))
        for theTopic in self.topic_list:
            #First, get the priorities:
            subParts = theTopic.split('|')
            priority = int(subParts[0])
            
            msgTypeParts = subParts[1].split('/')
            rosPkg = msgTypeParts[0]
            msgType = msgTypeParts[1]

            topicName = subParts[2]

            topicPath = list()
            if topicPrefix != '':
                topicPath.append(topicPrefix)
            if host != '':
                topicPath.append(host)
            topicPath.append(topicName)

            fullPath = '/'.join(topicPath)
                
            #msgClass = getattr(import_module('%s.msg' % rosPkg), '%s' % msgType)
            rospy.loginfo('Creating subscriber: %s, priority %d' % (fullPath, priority))

            #Need to use AnyMsg to get the _buff property (and avoiding having to know about every message type in use)
            self.hosts[host][theTopic] = rospy.Subscriber(fullPath, rospy.AnyMsg, self.onDataPresent, callback_args=(host, priority, topicName))
            
        #Create a generic serializer that accepts any topic:
        topicPath = list()
        if topicPrefix != '':
            topicPath.append(topicPrefix)
        if host != '':
            topicPath.append(host)
        topicPath.append('any')

        fullPath = '/'.join(topicPath)
        self.hosts[host]['any'] = rospy.Subscriber(fullPath, rospy.AnyMsg, self.onDataPresent, callback_args=(host, 1, 'any'))
       
                                                   
    def updateHosts(self):
        #Poll the mesh network for hosts
        #Verify we have the publishers publishing that we need
        #rospy.loginfo('Waiting for mesh node service')
        rospy.wait_for_service('getHosts')
        getHosts = rospy.ServiceProxy('getHosts', GetHosts)
        resp = getHosts() #EmptyRequest)
        for theHost in resp.hosts:
            if theHost not in self.hosts:
                self.addTopicsForHost(theHost)
        #print 'Got hosts:', resp
        
    def onDataPresent(self, payload, dest_args):
        #the payload may be typed or as rospy.AnyMsg
        msg = PriorityMeshData()
        msg.header.stamp = rospy.Time.now()
        if dest_args[0] == '':
            msg.host = 'all'
        else:
            msg.host = dest_args[0] #host
            
        msg.priority = int(dest_args[1]) #priority level
        msg.type = payload._connection_header['type']
        msg.topic = dest_args[2]
        
        #The payload is deliberately not typed at this level - it will be rehydrated by the ros_mesh_reader node
        msg.data =  payload._buff


        #Hand off to the mesh node for transmission
        self.data_pub.publish(msg)
    
if __name__ == '__main__':
    try:
        client = MeshWriter()
        client.run()
    except rospy.ROSInterruptException:
        pass
