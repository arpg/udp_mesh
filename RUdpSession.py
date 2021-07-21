#!/usr/bin/python2
'''
The RUdp I had been using was desingd for a 1->many distribution
We don't do that - this recycles some ideas with an eye to 1-1 distribution
Specifically, the use of a RUdpPeer class that manages acks and whatnot for a specific host
This will simplify the fragment tracking that the udp_mesh_node class had before
In particular, there's no need for SYN/DATA/FIN semantics - we can just blast packets out and ack them as needed
The dynamic window control is interesting - that controls the number of packets in flight at a given time


The existing rudp implementation didn't allow for out of order reception, instead blocking the receiver until the correct part of the message arrived. 

I also want to eliminate the global event queue thing - that's a recipe for a threading disaster
This code assumes that the higher-level code provides socket creation and selection semantics - this just sucks in complete packets


'''
import rospy
from MessageTypes import *
from MessageFragment import *
from enum import Enum
from Queue import Queue
from StringIO import *
from KeyHeap import *

import base64
import struct
import threading
from threading import Condition, RLock
from RUdpPeer import *
from udp_mesh.msg import *
import bz2

class MeshHost(object):
    def __init__(self, name, ip):
        self.ip = ip
        self.name = name
        self.lastSeen = rospy.Time.now()
    

class RUdpSessionManager(object):
    OFFLINE_TIMEOUT = 10.0 #seconds that can pass without a heartbeat before a peer is marked offline
    STATUS_INTERVAL = 0.5 #time between stat messages
    USE_COMPRESSION = False
    BROADCAST_WINDOWSIZE = 5 #number of broadcast packets that are sent per loop iteration
    
    def __init__(self, sock, port, broadcast_addr,
                 use_broadcast, broadcast_frag_max):
        self.sock = sock
        self.port = port
        self.broadcast_addr = broadcast_addr
        self.hosts = dict()
        self.peers = dict()
        self.onMessageComplete = None
        self.onPublishStats = None
        self.lastStatTime = rospy.Time.now()
        self.completeMessages = []
        self.lastSettings = None
        self.useBroadcast = use_broadcast
        self.broadcastFragMax = broadcast_frag_max
        
        self.broadcast_seq_num = 0x8000 #set the high bit for the broadcast msgs
        self.xmitQueue = KeyHeap(self.makeFragmentKey) #for the broadcast traffic
        self.broadcastWindowSize = RUdpSessionManager.BROADCAST_WINDOWSIZE
        
    def makeFragmentKey(self, frag):
        #For the priority queue, this creates a sortable key to order packets appropriately
        #Sequence and fragment number are both shorts
        #Priority is part of the messagefragment object, but not present in the datastream
        totalPri = (frag.priority << 48) | ((frag.seq & 0xffff) << 32) | (frag.frag & 0xffffffff) 
        return totalPri

    def updateSettings(self, msg):
        #Given a MeshSettings message, apply as necessary
        self.lastSettings = msg
        for peerAddr in self.peers:
            self.peers[peerAddr].setWindowSize(msg.windowSize)
            
    def setCompleteHandler(self, handler):
        self.onMessageComplete = handler
        
    def setStatsHandler(self, handler):
        self.onPublishStats = handler
        
    def handlePacket(self, srcAddr, frag):
        completeMsg = None
        #Route to the correct peer for action
        try:
            completeMsg = self.peers[srcAddr].handlePacket(frag)
        except KeyError:
            #We don't have a peer for this fragment for some reason
            #Most likely because the heartbeat hasn't reached here yet, so we don't have peer information
            raise 

        if not completeMsg is None:
            self.completeMessages.append(completeMsg)
            
    def addPeer(self, host, addr):

        if addr not in self.peers:
            self.peers[addr] = RUdpPeer(host, addr, self.port, self.sock, self,
                                        compression = self.USE_COMPRESSION)
            if self.lastSettings is not None:
                #Apply the current settings as needed
                self.peers[addr].setWindowSize(self.lastSettings.windowSize)
            

        self.peers[addr].lastSeen = rospy.Time.now()
        self.peers[addr].offline = False
        
        #print 'Marking %s online' % host
        #This dict is used to reverse map the ip / hostname so we don't have to do lookups for every message
        self.hosts[host] = MeshHost(host, addr)
        self.hosts[host].lastSeen = rospy.Time.now()


    def getPeers(self):
        peers = []
        for addr in self.peers:
            peers.append((self.peers[addr].host, addr))
        return peers

    def sendRawMessage(self, host, data):
        rawPriority = 0
        if host != 'all':
            try:
                self.peers[self.hosts[host].ip].sendMessage(data, rawPriority)
            except KeyError as err:
                rospy.loginfo('RUdpSession: Ignoring raw message for unknown host: %s' % (host))
        else: #handle broadcast traffic
            if self.useBroadcast:
                 self.sock.sendto(data, (self.broadcast_addr, self.port))
            else:
                #Send unicast to everybody to emulate broadcast
                for addr in self.peers:
                    #Normally, we wouldn't want to try sending to offline hosts
                    #however, without broadcast enabled, this is the only way that a partner host will find us
                    #if self.peers[addr].offline:
                    #print 'Ignoring message for offline host %s' % msg.host
                    #    continue
                    #Direct send via socket - we don't want the peer filling up its inflight queue
                    self.sock.sendto(data, (addr, self.port))

    def sendMessage(self, msg):
        #Given a PriorityMeshData ros message, post it to the correct peer for transmission
        #print 'Pushing message to:', msg.host

        #Serialize the message into a buffer:
        #Serialize the PriorityMeshData message for transmission:
        buf = StringIO.StringIO()
        msg.serialize(buf)
        data = buf.getvalue()
        
        if msg.host == 'all':
            #Handle broadcast messages
            if len(data) < MessageFragment.FRAGMENT_MAX_SIZE*self.broadcastFragMax and self.useBroadcast:
                #Make an intelligent decision: if the size of message exceeds the mtu, use unicast semantic instead
                #Only send broadcast when enabled
                self.sendMessageBroadcast(data, msg.priority)
                return
            else: #Queue up unicast to all online hosts:
                for addr in self.peers:
                    if self.peers[addr].offline:
                        #print 'Ignoring message for offline host %s' % msg.host
                        continue
                    if self.USE_COMPRESSION:
                        sendBuf = bz2.compress(data)
                    else:
                        sendBuf = data
                        self.peers[addr].sendMessage(sendBuf, msg.priority)
                return

        #Handle the unicast cast
        #Don't push packets to offline hosts
        if self.peers[self.hosts[msg.host].ip].offline:
            #print 'Ignoring message for offline host %s' % msg.host
            return

        if self.USE_COMPRESSION:
            sendBuf = bz2.compress(data)
        else:
            sendBuf = data
            
        self.peers[self.hosts[msg.host].ip].sendMessage(sendBuf, msg.priority)
        
    def sendMessageBroadcast(self, data, priority):
        #Send to all peers using the broadcast address
        #Since the high bit of the seq is used to indicate broadcast traffic, the session manager can handle the incrementing without consulting the peers
        #However, broadcast sends do not have acknowledgements...

        numFrags = int(len(data) / MessageFragment.FRAGMENT_MAX_SIZE) + 1
        self.broadcast_seq_num = ((self.broadcast_seq_num + 1) & 0xffff) | 0x8000 #high bit seq numbers are reserved for broadcasts
    
        thisSeq = self.broadcast_seq_num
        #print 'Pushing seq: %d' % thisSeq
        for fragIndex in range(numFrags):
            fragStart = fragIndex*MessageFragment.FRAGMENT_MAX_SIZE

            theFrag = MessageFragment(MessageTypes.PAYLOAD_NOACK)
            
            theFrag.encodePayload(thisSeq, fragIndex, numFrags, data[fragStart:fragStart + MessageFragment.FRAGMENT_MAX_SIZE], priority)

            #print 'Created frag with seq %d frag %d' % (theFrag.seq, theFrag.frag)
            self.xmitQueue.put(theFrag)
            
                
    def loopOnce(self):
        #Process all active peer sessions, retransmitting / abandoning as needed
        curTime = rospy.Time.now()
        netStats = NetworkStats()
        netStats.header.stamp = rospy.Time.now()
        
        for peer in self.peers:
            stats = self.peers[peer].getStats()
            netStats.peers.append(stats)
            if stats.offline:
                continue
            
            if curTime - stats.lastSeen > rospy.Duration(self.OFFLINE_TIMEOUT):
                #print 'Marking %s offline' % peer
                self.peers[peer].offline = True
                self.peers[peer].flush()
                
            #Check for stale packets, marking offline
            if self.peers[peer].checkStaleness(self.OFFLINE_TIMEOUT):
                self.peers[peer].offline = True
                self.peers[peer].flush()

            #Check for lingering fragmented messages, where the fragments have taken too long to arrive
            self.peers[peer].pruneIncompleteMessages(self.OFFLINE_TIMEOUT)
            
        #Push monitoring info if needed
        if curTime - self.lastStatTime > rospy.Duration(self.STATUS_INTERVAL):
            self.lastStatTime = curTime
            if self.onPublishStats is not None:
                self.onPublishStats(netStats)
            
        #Ensure a callback is set
        if self.onMessageComplete is None:
            return
        
        #Hit the callback for every complete message requiring dispatch
        for msg in self.completeMessages:
            self.onMessageComplete(msg)

        #Reset for the next iteration
        self.completeMessages = []
    
        #Send some broadcast traffic if present:
        broadcastsSent = 0
        while broadcastsSent < self.broadcastWindowSize:
            if self.xmitQueue.empty():
                break;

            toSend = self.xmitQueue.get()
            toSend.lastXmitTime = rospy.Time.now()
            self.sock.sendto(toSend.getBinary(),
                             (self.broadcast_addr, self.port))
            broadcastsSent += 1
