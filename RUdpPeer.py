#!/usr/bin/python2
'''
Peer management
This class handles sending / receiving
The sending part handles window management and retransmits
The recv part handles acks and reassembly, then spits out to parent for dispatch

To implement the QoS portion, we have a queue at each priority level
One message at each priority is allowed to be inflight at a time

'''
from MessageTypes import *
from MessageFragment import *
from enum import Enum
from Queue import Queue
from KeyHeap import *
import rospy
from udp_mesh.msg import *
import bz2

class RUdpPeer(object):
    WINDOW_DEFAULT_SIZE = 3 #number of packets that can be inflight for a given priority message at any time

    def __init__(self, host, addr, port, sock, parent, compression=False, window_size = WINDOW_DEFAULT_SIZE):
        self.host = host
        self.addr = addr
        self.port = port
        self.sock = sock
        self.parent = parent
        self.seqNum = 0
        self.xmitQueue = KeyHeap(self.makeFragmentKey)
        self.inFlight = {} #packets that are awaiting an ACK to be cleared from this list, indexed by fragment key
        self.partialRecv = {} #dict, keyed by seq num, of a list of frags
        self.stats = PeerStats()
        self.stats.host = self.host
        self.stats.addr = self.addr

        self.staleInflightCount = 0
        self.incompleteCount = 0
        
        self.use_compression = compression
        self.window_size = window_size
        self.offline = False
        
    def setWindowSize(self, size):
        self.window_size = size
        
    def makeFragmentKey(self, frag):
        #For the priority queue, this creates a sortable key to order packets appropriately
        #Sequence and fragment number are both shorts
        #Priority is part of the messagefragment object, but not present in the datastream
        totalPri = (frag.priority << 48) | ((frag.seq & 0xffff) << 32) | (frag.frag & 0xffffffff) 
        return totalPri

    def handlePacket(self, msg):
        #Given some packet for this peer, figure out what to do with it
        #Returns a MeshData for dispatch if needed

        #Mark this one online, we just got a message
        self.offline = False
        
        #We only get two types of packets - an ack or a payload
        #It was already decoded for us

        if msg.msgType == MessageTypes.ACK:
            #Find the corresponding message in the inflight queue
            msgKey = self.makeFragmentKey(msg) #the priority will be set to zero, since priorities are not in the datastream
            #print 'Got ack, seq: %d frag:%d' % (msg.seq, msg.frag)
            for theKey in self.inFlight.keys():
                if theKey & 0xffffffffffff == msgKey: #(lower 48 bits)
                    #Found the right one, remove from the inflight list
                    sentPkt = self.inFlight.pop(theKey)
                    #print 'removed completed packet'
                    self.stats.lastRTT = rospy.Time.now() - sentPkt.lastXmitTime
                    #Since this was an ack, then transmit the next packet in the priority queue
                    self.fillInFlight()
                    break;

            #if the peer goes offline before the ack is received, the frag will linger in the inFlight
            #When it comes back online, those inFlight will be resent, just in case
            
            return None
        elif msg.msgType == MessageTypes.PAYLOAD or msg.msgType == MessageTypes.PAYLOAD_NOACK:
            '''
            Payloads require more careful attention
            Packets may arrive out of order or not at all
            Design decision: Hold onto everything
            
            By design, this peer transmitted the packets - only need to track seq and frag numbers

            Consider the case where a receiver goes offline before the ack is received by the sender
            The receiver merrily completes and publishes the packet
            But the sender will retain that packet in its inflight queue

            '''
            if msg.msgType == MessageTypes.PAYLOAD:
                #First, send an ack to the sender without waiting for the xmit queue
                ackFrag = MessageFragment(MessageTypes.ACK)
                #print 'Sending ack, seq: %d frag %d' % (msg.seq, msg.frag)
                ackFrag.encodeAck(msg.seq, msg.frag, msg.fragTotal)
                self.sock.sendto(ackFrag.getBinary(),
                                 (self.addr, self.port))

            #print 'Got data, seq: %d frag %d' % (msg.seq, msg.frag)
            if msg.seq not in self.partialRecv:
                #create the list of fragments
                #Every message contains the frag index, along with the total number of frags
                #Only one is needed to create the complete set
                #Create a list of 'None's, where the index into the list is the fragment number
                self.partialRecv[msg.seq] = [None] * msg.fragTotal

            #We're guaranteed that there are a set of entries for this particular message now
            #Log the recv time so that we can purge stale partial messages
            
            msg.recvTime = rospy.Time.now()
            self.partialRecv[msg.seq][msg.frag] = msg

            
            #Check to see if this message is now complete (all fragments received)

            fragTotal = 0        
            for theFrag in self.partialRecv[msg.seq]:
                if theFrag is not None:
                    fragTotal += 1

            #Now, see if we have the entire message
            if fragTotal == msg.fragTotal:
                completeMessage = True
            else:
                completeMessage = False

        #We're still awaiting more fragments to show up
        if not completeMessage:
            return None

        #otherwise, reassemble the message and publish
        #Push a meshdata message
        msgOutput = PriorityMeshData()
        rawPayload = self.reassembleMessageFragments(self.partialRecv[msg.seq])

        if self.use_compression:
            msgOutput.deserialize(bz2.decompress(rawPayload))
        else:
            msgOutput.deserialize(rawPayload)
        #Fix up the metadata - this was initially addressed to us, rewrite the host field to represent where this was from instead
        msgOutput.host = self.host
        #rospy.loginfo('Reassembled: %d message: %s', len(msg.data), msg.data)
       

        #Delete this sequence from the received items
        self.partialRecv[msg.seq] = None #release buffers, etc
        del self.partialRecv[msg.seq]

        return msgOutput

    def reassembleMessageFragments(self, frags):
        #Given a list of message fragments, reassemble and return the complete message
        buf = io.BytesIO()
        writer = io.BufferedWriter(buf)

        try:
            fragTotal = frags[0].fragTotal #every message has at least one fragment
        except KeyError:
            pdb.set_trace()
            
        for theFrag in range(fragTotal):
            writer.write(frags[theFrag].data) #write fragments in order
        writer.flush()
        return buf.getvalue()
        
    def makeMessageFragments(self, data, priority):
        #Given a honkin message and a dest, make some MessageFragments out of it
        numFrags = int(len(data) / MessageFragment.FRAGMENT_MAX_SIZE) + 1
        frags = list()
        self.seqNum = (self.seqNum + 1) & 0x7fff #high bit seq numbers are reserved for broadcasts
        #print 'Using seq: %d' % self.seqNum
        thisSeq = self.seqNum
        for fragIndex in range(numFrags):
            fragStart = fragIndex*MessageFragment.FRAGMENT_MAX_SIZE

            theFrag = MessageFragment(MessageTypes.PAYLOAD)
            theFrag.encodePayload(thisSeq, fragIndex, numFrags, data[fragStart:fragStart + MessageFragment.FRAGMENT_MAX_SIZE], priority)
            frags.append(theFrag)
            #print 'Created frag with seq %d frag %d' % (theFrag.seq, theFrag.frag)
        return frags


                            
    def sendMessage(self, data, priority):
        '''
        Fragment the message and pass to rudp for transmission
        '''
        #Maintain a queue of things to send - this is the sliding window
        #The queue is arranged by priority - high pri things go out before low-pri things
        #This has to be on a per-fragment basis, to avoid something like a map clogging up the pipe 

        #PriorityQueue - use the fragment priority, then by sequence, then by fragment num
        #But I still need to maintain access to be able to remove an item from the priority queue when an ack is received

        #Take an item from the priority queue and put it into the inflight tracker
        #When an ack is received, remove it from the inflight tracker and get the next item from the queue
        
        for frag in self.makeMessageFragments(data, priority):
            #Post each fragment to the comm subsystem
            #The fragment knows its sequence number and fragment number, as well as the total number of fragments

            #Put it in the priority queue for transmission
            #print 'Queuing seq: %d frag: %d' % (frag.seq, frag.frag)
            self.xmitQueue.put(frag)

        #print 'Fragments posted'
        self.fillInFlight()
        
    def fillInFlight(self):
        #Fill the xmit queue initially if needed

        while len(self.inFlight) < self.window_size:
            if self.xmitQueue.empty():
                break;
            toSend = self.xmitQueue.get()
            toSend.lastXmitTime = rospy.Time.now()

            self.inFlight[self.makeFragmentKey(toSend)] = toSend
            #print 'Sending seq: %d frag: %d' % (toSend.seq, toSend.frag)
            self.sock.sendto(toSend.getBinary(),
                             (self.addr, self.port))

    #When a peer comes back online, try to resume messages
    #The xmit queue is fine, since they were never sent
    #but the inflight may be inconsistent and have dropped en route
    #So no acks might ever be received
    #Resend the inflights, just in case
    
    def resendInFlight(self):
        for frag in self.inFlight:
            self.sock.sendto(frag.getBinary(),
                             (self.addr, self.port))
            frag.lastXmitTime = rospy.Time.now()

    def flush(self):
        #When a host goes offline, flush the inflight and the xmit queue - there's not a great way to restart things
        self.inFlight = {}
        self.xmitQueue = KeyHeap(self.makeFragmentKey)

    #Are the inflight messages too old? 
    def checkStaleness(self, timeout):
        curTime = rospy.Time.now()
        for msgKey in self.inFlight:
            if curTime - self.inFlight[msgKey].lastXmitTime > rospy.Duration(timeout):
                self.staleInflightCount += 1
                return True
        return False

    #Remove partial messages that are missing fragments and haven't been touched in a bit
    def pruneIncompleteMessages(self, timeout):
        curTime = rospy.Time.now()
        stale = True
        toDelete = []
        for seq in self.partialRecv:
            for frag in self.partialRecv[seq]:
                if frag is not None:
                    #keep the message around if at least one frag is current
                    if curTime - frag.recvTime < rospy.Duration(timeout):
                        stale = False
                        break
            if stale:
                for frag in self.partialRecv[seq]:
                    frag = None
                toDelete.append(seq)

        #Since I can't delete something I'm iterating over 
        for seq in toDelete:        
            del self.partialRecv[seq]
            self.incompleteCount += 1
                    

        
    #Stats are a ROS message
    def getStats(self):
        self.stats.xmitDepth = self.xmitQueue.qsize()
        self.stats.seqNum = self.seqNum
        self.stats.offline = self.offline
        self.stats.lastSeen = self.lastSeen
        self.stats.incompleteCount = self.incompleteCount
        self.stats.staleInflightCount = self.staleInflightCount
        return self.stats
