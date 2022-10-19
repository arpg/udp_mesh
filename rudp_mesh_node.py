#!/usr/bin/python3
#ROS RUDP Mesh Interface
'''
This is an adapter to take in MeshData messages and send them to their destinations over UDP
Based on the esp_mesh_node and udp_mesh_node
This node uses the RUdp stack from kth-python-Rudp to implement reliable udp transport
Implication is that this node is responsible for packaging ROS messages and delivering them to transport

Design choice of tying into multimaster-discovery or just running our own
With the wifi, multicast essentially becomes broadcast anyway- so why not just use broadcast and be done with it?

Goals:
Having a QoS interface, so that we make sure high-priority messages are transmitted ahead of lo-pri data

Assumptions:
Only one message to a given host is in flight at any time

Operation:
Receive a MeshData from the mesh_reader node that has addressing and topic info
Check to make sure that the destination host is online and available
Send the MeshData:
1. Send begin with meta header info
2. Fragment and send data packets as needed. RUdp handles acks and such
ACK Timeout? Mark the host offline, continue



Following the VsFtp example, we have several packet types that can be transmitted over RUdp:

msg_begin (provides type / topic information) 
msg_data (includes a last_packet flag to indicate final packet of this complete message)

Host announcements don't use RUdp - there's no need to do reliable acking and such for a single packet
Host (doubles as a heartbeat)
For Host messages, re-use the existing UDP broadcast plumbing that we already wrote

The UDP Mesh Node has to handle fragmentation and reassembly - but RUdp will make sure that the fragments arrive



Startup
1. Set the inbound RUdp socket to receive
Loop - call the EventLoop in its own thread


RUDP sequence:
Peer detected? open a socket, set to state_listening
Each message:
1. Syn / ack / data / fin sequence - reset to listening after each message
Timeout? Reset to listening 
Example application tears down the socket after every transmission - I want to leave the channel open while we're still getting packets thru




Publications:
1. Meta data - hosts and online / offline status


 
In UDP-land, addresses are hostnames or IPs, not MACs
Relies on ARP / an IP stack
Can we do UDP broadcasts to avoid having to run DNS?
'''

#Enable Py3 support
#from __future__ import division
#from __future__ import print_function
#from future import standard_library
#standard_library.install_aliases()
#from builtins import str
#from past.utils import old_div
#from builtins import object

import rospy
from std_msgs.msg import String
import sys, logging, os, socket, time
from optparse import OptionParser
import select
import threading
from threading import Condition, RLock
import numpy as np

import fcntl
import struct
import logging
from errno import EINTR

from io import StringIO
import io
import pdb
import time
from udp_mesh.srv import *
from udp_mesh.msg import *

from enum import Enum
from queue import Queue
import base64
import struct

from MessageTypes import *
from MessageFragment import *
from RUdpSession import *

'''
ROS-ESP Mesh node bridge 
This node bridges the idea of raw data streams to the esp mesh layer
From this node's perspective, it accepts MeshData messages that are a complete envelope
with host and payload for the dest

This handles hostname <-> MAC mapping services as well

Now with fragmentation / readback / retr / ack support!

'''

def makeUInt8(arg):
    return struct.pack("B", arg)

#Simple 1-byte checksum for the fragment data
# returns total mod 256 as checksum
# input - string

   
class MeshCmds(Enum):
    SET_HOSTNAME =1
    SEND_MSG = 3
    PING_HOST = 8
    SEND_FRAG = 9
    UPDATE_SETTINGS = 10
    

class MeshCmd(object):
    def __init__(self, cmdType, arg=None):
        self.cmdType = cmdType
        self.arg = arg
        self.readyBell = Condition()
        self.response = None
    def wait(self):
        self.readyBell.acquire()
        self.readyBell.wait()
        self.readyBell.release()
    def notify(self):
        self.readyBell.acquire()
        self.readyBell.notify()
        self.readyBell.release()


class RUDPMeshNode(object):
    DEFAULT_DATA_PORT = 8700
    SOCK_TIMEOUT = 0.01 #floating point number in seconds
    MAX_BLKSIZE = 1400
    DEFAULT_INTERVAL = 1.0 #heartbeat announce interval
    
    def __init__(self):
        rospy.init_node('rudp_mesh', anonymous=True)
        self.data_port = rospy.get_param('~data_port', self.DEFAULT_DATA_PORT)
        self.heartbeat_interval = rospy.get_param('~heartbeat_interval', self.DEFAULT_INTERVAL)
        self.hostname =  rospy.get_param('~hostname')
        if len(self.hostname) == 0:
            self.hostname =  socket.gethostname()
        self.interface = rospy.get_param('~interface', 'wlan0')
        self.socket_ip= self.get_interface_ip(self.interface)
        self.socket_mask = self.get_netmask(self.interface)
        self.broadcast_addr = self.get_broadcast_addr()
        self.broadcast_frag_max = rospy.get_param('~broadcast_frag_max', 5)
        self.use_broadcast = rospy.get_param('~use_broadcast', True)
        

        if self.use_broadcast:
            rospy.loginfo('Using broadcast address: %s' % self.broadcast_addr)
        
        self.cmdQ = Queue()

        
        self.openPort()
        self.sessionManager = RUdpSessionManager(self.sock, self.data_port,
                                                 self.broadcast_addr, self.use_broadcast,
                                                 self.broadcast_frag_max)
        self.sessionManager.setCompleteHandler(self.dispatchMessage)

        self.settings_sub = rospy.Subscriber('mesh_settings', MeshSettings, self.onMeshSettings)
        self.data_sub = rospy.Subscriber('outbound_data', PriorityMeshData, self.onDataPresent)
        self.data_pub = rospy.Publisher('inbound_data', PriorityMeshData, queue_size=10)

        self.hosts_pub = rospy.Publisher('hosts', HostEntry, queue_size=10)
        self.status_pub = rospy.Publisher('status', NetworkStats, queue_size=10)
        
        self.sessionManager.setStatsHandler(self.status_pub.publish)
        
        self.nameAnnounce = rospy.Timer(rospy.Duration(self.heartbeat_interval), self.registerHostname)

        #Process the static hosts from the launch file
        self.host_list = rospy.get_param('~host_list', [])
        for theHost in self.host_list:
            try:
                hostIP = socket.gethostbyname(theHost)
                if hostIP != '127.0.0.1':
                    self.addHostname(theHost, hostIP)
            except socket.gaierror as err:
                rospy.loginfo('Not adding static host %s, unable to resolve' % theHost)

        #Wait until the static hosts are created before advertising the service - otherwise, the mesh writer misses them
        self.hosts_srv = rospy.Service('getHosts', GetHosts, self.onGetHostsSvc)

    def get_netmask(self, ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #SIOCGIFNETMASK
        return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x891b, struct.pack('256s',ifname.encode()))[20:24])
    
    def get_interface_ip(self,ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
                s.fileno(),
                0x8915,  # SIOCGIFADDR
                struct.pack('256s', bytes(ifname[:15].encode()))
                # Python 2.7: remove the second argument for the bytes call
            )[20:24])

    def get_lan_ip(self):
        ip = socket.gethostbyname(socket.gethostname())
        if ip.startswith("127.") and os.name != "nt":
            interfaces = ["eth0","eth1"]
            interfaces.append(self.interface)
            for ifname in interfaces:
                try:
                    ip = self.get_interface_ip(ifname)
                    break;
                except IOError:
                    pass
        return ip

    def get_broadcast_addr(self):

        local_octets = self.socket_ip.split('.')
        mask_octets = self.socket_mask.split('.')
        
        #mask off the network, figure out the broadcast address
        #invert the netmask, or the ip to get broadcast
        #That would be the right way. Our networks are standard class B or C
        #Just check whether we are a 0 or 255

        #When using a VPN tunnel, the mask will be 255.255.255.255 - which means that there isn't a broadcast address
        #No need to fake a class C address in this case - note that one must be running a udp_broadcast_relay on the remote tunnel endpoint
        #that forwards these pseudo-broadcasts from the tunnel interface to the LAN interface (normally, this is a routing function
        #handled by iptables
        #One such program is: https://github.com/nomeata/udp-broadcast-relay.git
        #Might have to be cross-compiled for the architecture of one's router
        
        broadcastIP = '.'.join(local_octets[0:2])
        
        for i in range(2,4):
            if mask_octets[i] == '0':
                broadcastIP = '.'.join([broadcastIP, '255'])
            else:
                broadcastIP = '.'.join([broadcastIP, local_octets[i]])
                         
        broadcastIP = '255.255.255.255'
        return broadcastIP
    
        
    def openPort(self):
        listenip = '0.0.0.0' #self.socket_ip
        rospy.loginfo("Server requested on ip %s, port %s" % (listenip, self.data_port))
        try:
            # FIXME - sockets should be non-blocking
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self.sock.bind((listenip, self.data_port))
            _, self.listenport = self.sock.getsockname()

        except socket.error as err:
            # Reraise it for now.
            raise err
        return

    def announceName(self):
        #Send a UDP broadcast with a message type of HOST to announce our name and IP to the network
        #prepare a mesh name message:
        msg = MessageFragment(MessageTypes.HOST)
        msg.encodeHost(self.hostname, self.socket_ip)
        self.sessionManager.sendRawMessage('all', msg.getBinary()) 

    #Since the rospy timers don't occur on the main thread, marshal to the main thread for action
    def registerHostname(self, evt):
        self.cmdQ.put(MeshCmd(MeshCmds.SET_HOSTNAME, self.hostname))
        
    def onGetHostsSvc(self, req):
        #Pull from the session manager

        res = GetHostsResponse()

        #This will need to pull from the SessionManager, which maintains the list of peers
        peers = self.sessionManager.getPeers()
        for peer in peers:
            res.hosts.append(peer[0])
            res.macs.append(peer[1])
        return res

        
    def runCommand(self, cmd):
        if cmd.cmdType == MeshCmds.SET_HOSTNAME:
            #rospy.loginfo('Registering as hostname: %s', self.hostname)
            self.announceName()
            
        elif cmd.cmdType == MeshCmds.SEND_MSG:
            #rospy.loginfo('Sending message to: %s' % cmd.arg.host)
            #rospy.loginfo(cmd.arg)
            self.sessionManager.sendMessage(cmd.arg)
        elif cmd.cmdType == MeshCmds.UPDATE_SETTINGS:
            self.updateSettings(cmd.arg)
            
        else:
            rospy.loginfo('Unknown cmd: %s', cmd.cmdType)


    def statusUpdate(self, evt):
        #Enqueue a sysinfo request
        self.cmdQ.put(MeshCmd(MeshCmds.GET_SYSINFO))

        
    def checkSockets(self):
        # Build the inputlist array of sockets to select() on - just the main one for now
        inputlist = []
        inputlist.append(self.sock)

        
        #rospy.loginfo("Performing select on this inputlist: %s", inputlist)
        try:
            readyinput, readyoutput, readyspecial = select.select(inputlist, [], [], self.SOCK_TIMEOUT)
        except select.error as err:
            if err[0] == EINTR:
                # Interrupted system call
                rospy.loginfo("Interrupted syscall, retrying")
                return
            else:
                raise
        for readysock in readyinput:
            # Is the traffic on the main server socket? 
            if readysock == self.sock:
                #rospy.loginfo("Data ready on our  socket")
                buffer, (raddress, rport) = self.sock.recvfrom(self.MAX_BLKSIZE)
                #rospy.loginfo("Read %d bytes", len(buffer))

                #Ignore the loopbacks
                if raddress != self.socket_ip:
                    self.handlePacket(raddress, rport, buffer)

    def run(self):
        #Main loop of the node

        while not rospy.is_shutdown():
            #Command q is still necessary so that a socket is only ever touched by one thread, the main thread
            #ROS callbacks are on separate threads and will corrupt state eventually
            if not self.cmdQ.empty():
                self.runCommand(self.cmdQ.get())
                
            #Check for heartbeat- this includes a select with a timeout, so no need for a delay
            self.checkSockets()

            #Loop the session manager once
            self.sessionManager.loopOnce()
            
        if self.sock:
            self.sock.close()

    
    def handlePacket(self, src, srcPort, pkt):
        #Can significantly simplify the packet structure by using a struct to pack/unpack fields        
        if len(pkt) > 0:
            #Interpret the line:
            #Fallback: no action, just pass a ''
            #print 'Packet: %s' % pkt

            #Packets are all of class MessageFragment-encoded
            msg = MessageFragment()
            msg.decode(pkt)

            if msg.msgType == MessageTypes.HOST:
                self.processHost(src, msg)
            elif msg.msgType == MessageTypes.ACK or msg.msgType ==  MessageTypes.PAYLOAD or msg.msgType ==  MessageTypes.PAYLOAD_NOACK:
                #Hand off to the session manager for further processing
                try:
                    self.sessionManager.handlePacket(src, msg)
                except KeyError:
                    rospy.loginfo("No heartbeat from %s before data, ignoring" % src)
            else:
                rospy.loginfo('Unknown message type:%s', msg.msgType)

    def dispatchMessage(self, msg):
        #Called when a complete message is ready to go
        #Needs to be written to the correct output pipe - the mesh_reader takes care of that part
        #Note that this will be called from the socket thread
        #But that's OK, ROS doesn't require publication from any particular thread
        #rospy.loginfo("Dispatching complete message")
        self.data_pub.publish(msg)
    
    def publishSysinfo(self):
        
        #Push a sysinfo message
        '''
        msg = MeshInfo()
        msg.header.stamp = rospy.Time.now()
        msg.channel = int(channel[1])
        msg.layer = int(layer[1])
        msg.own_mac = own_mac[1]
        msg.parent_mac = parent_mac[1]
        msg.rssi = int(rssi[1])
        msg.nodecount = int(nodecount[1])
        msg.heap = int(heap[1])
        for child in self.childNodes:
            msg.children.append(child)

        #wipe the existing child records, they'll be recreated
        self.childNodes = []
        
        self.status_pub.publish(msg)
        '''
        
    def processHost(self, src, msg):
        #rospy.loginfo('Adding host: %s with ip:%s to table', msg.hostname, msg.ip)
        #Avoid creating the potential for a loopback 
        if msg.hostname != self.hostname:
            self.addHostname(msg.hostname, msg.ip)

    def addHostname(self, host, mac):
        self.sessionManager.addPeer(host, mac)
        
        msg = HostEntry()
        msg.header.stamp = rospy.Time.now()
        msg.host = host.decode()
        msg.mac = mac.decode()
        self.hosts_pub.publish(msg)
        
    def onDataPresent(self, msg):
        #print 'Got meshData, Sending message to:', msg.host
        #prepare a transmission for the system
        #Data in this case is a serialized Python message
        self.cmdQ.put(MeshCmd(MeshCmds.SEND_MSG, msg))

    def updateSettings(self, msg):
        self.sessionManager.updateSettings(msg)
            
    def onMeshSettings(self, msg):
        self.cmdQ.put(MeshCmd(MeshCmds.UPDATE_SETTINGS, msg))
        
import cProfile

if __name__ == '__main__':
    print("Starting RUdpMeshNode")
    try:
        client = RUDPMeshNode()
        #cProfile.run('client.run()')
        client.run()
    except rospy.ROSInterruptException:
        print("ROS exception!")
        pass
