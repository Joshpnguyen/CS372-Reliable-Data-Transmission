from segment import Segment
import copy

# #################################################################################################################### #
# RDTLayer                                                                                                             #
#                                                                                                                      #
# Description:                                                                                                         #
# The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable         #
# channel.                                                                                                             #
#                                                                                                                      #
#                                                                                                                      #
# Notes:                                                                                                               #
# This file is meant to be changed.                                                                                    #
#                                                                                                                      #
#                                                                                                                      #
# #################################################################################################################### #


class RDTLayer(object):
    # ################################################################################################################ #
    # Class Scope Variables                                                                                            #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    DATA_LENGTH = 4 # in characters                     # The length of the string data that will be sent per packet...
    FLOW_CONTROL_WIN_SIZE = 15 # in characters          # Receive window size for flow-control
    sendChannel = None
    receiveChannel = None
    dataToSend = ''
    currentIteration = 0                                # Use this for segment 'timeouts'
    # Add items as needed

    # ################################################################################################################ #
    # __init__()                                                                                                       #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        self.sendBase = 0
        self.lastAck = 0
        self.dataReceived = ''  # keep track of data received server-side
        self.received = []
        self.pipeline = 0
        self.buffer = {}  # keep track of the time out for each segment
        self.timeouts = 0


    # ################################################################################################################ #
    # setSendChannel()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable sending lower-layer channel                                                 #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setSendChannel(self, channel):
        self.sendChannel = channel

    # ################################################################################################################ #
    # setReceiveChannel()                                                                                              #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable receiving lower-layer channel                                               #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setReceiveChannel(self, channel):
        self.receiveChannel = channel

    # ################################################################################################################ #
    # setDataToSend()                                                                                                  #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the string data to send                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def setDataToSend(self,data):
        self.dataToSend = data

    # ################################################################################################################ #
    # getDataReceived()                                                                                                #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to get the currently received and buffered string data, in order                                  #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def getDataReceived(self):
        # ############################################################################################################ #
        # Identify the data that has been received...

        print('getDataReceived(): Complete this...')
        # ############################################################################################################ #
        #data = ''.join(self.dataReceived[x] for x in sorted(self.dataReceived))
        return self.dataReceived
        #return data[0:self.rcvBase]

    # ################################################################################################################ #
    # processData()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # "timeslice". Called by main once per iteration                                                                   #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processData(self):
        self.currentIteration += 1
        self.processSend()
        self.processReceiveAndSendRespond()

    # ################################################################################################################ #
    # processSend()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment sending tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processSend(self):

        if self.dataToSend == '':  # if no data to send, then it is the server, skip
            return

        if self.sendBase == self.lastAck:
            while self.pipeline < 3:
                segmentSend = Segment()

                # #################################################################################################### #
                print('processSend(): Complete this...')

                # You should pipeline segments to fit the flow-control window
                # The flow-control window is the constant RDTLayer.FLOW_CONTROL_WIN_SIZE
                # The maximum data that you can send in a segment is RDTLayer.DATA_LENGTH
                # These constants are given in # characters

                # Somewhere in here you will be creating data segments to send.
                # The data is just part of the entire string that you are trying to send.
                # The seqnum is the sequence number for the segment (in character number, not bytes)

                seqnum = self.sendBase
                data = self.dataToSend[seqnum:seqnum+self.DATA_LENGTH]

                # #################################################################################################### #
                # Display sending segment
                segmentSend.setData(seqnum,data)
                print("Sending segment: ", segmentSend.to_string())

                self.sendBase += 4
                self.buffer[seqnum] = 0  # start timeout counter
                # Use the unreliable sendChannel to send the segment
                copySeg = copy.deepcopy(segmentSend)
                self.sendChannel.send(copySeg)

                self.pipeline += 1

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #
    #                                                                                                                  #
    #                                                                                                                  #
    # ################################################################################################################ #
    def processReceiveAndSendRespond(self):

        # This call returns a list of incoming segments (see Segment class)...
        listIncomingSegments = self.receiveChannel.receive()

        # ############################################################################################################ #
        # What segments have been received?
        # How will you get them back in order?
        # This is where a majority of your logic will be implemented
        print('processReceive(): Complete this...')

        if self.dataToSend != '':  # client side receive
            listIncomingSegments.sort(key=lambda x: x.acknum)  # sort received packets
            acknum = -1  # if -1 then no missed/dropped packets
            for pkt in listIncomingSegments:
                if pkt.acknum == self.lastAck + 4 and self.lastAck not in self.received:  # for acks that are expected and sequential
                    self.pipeline -= 1
                    self.received.append(self.lastAck)
                    self.buffer.pop(self.lastAck)
                    self.lastAck += 4
                else:
                    acknum = pkt.acknum

            for seqnum, count in self.buffer.items():  # check segment timeouts
                if self.currentIteration - count > 4 and acknum == -1:  # if timeout, then send packet
                    self.timeouts += 1
                    acknum = seqnum
                    break
                self.buffer[seqnum] += 1

            # send packets

            if acknum != -1:
                for seq in range(acknum, acknum+9, 4):
                    data = self.dataToSend[seq:seq + self.DATA_LENGTH]
                    segmentSend = Segment()

                    # Display sending segment
                    segmentSend.setData(seq, data)
                    print("Sending segment: ", segmentSend.to_string())

                    # Use the unreliable sendChannel to send the segment
                    self.sendChannel.send(segmentSend)

        else:  # server side receive
            if listIncomingSegments:
                listIncomingSegments.sort(key=lambda x: x.seqnum)  # sort received packets
                for pkt in listIncomingSegments:
                    segmentAck = Segment()  # Segment acknowledging packet(s) received

                    if pkt.seqnum < len(self.dataReceived):  # check if packet already in dataReceived
                        continue

                    if not pkt.checkChecksum():  # check checksum for each packet
                        continue

                    elif pkt.seqnum == self.lastAck:  # for correct expected packets
                        # How do you respond to what you have received?
                        # How can you tell data segments apart from ack segemnts?
                        print('processReceive(): Complete this...')
                        print("Received segment:", pkt.payload)
                        self.lastAck += 4
                        # Somewhere in here you will be setting the contents of the ack segments to send.
                        # The goal is to employ cumulative ack, just like TCP does...
                        acknum = self.lastAck

                        self.dataReceived += pkt.payload  # add data to received

                    else:  # for missed acks
                        acknum = self.lastAck

                    # Display response segment
                    segmentAck.setAck(acknum)
                    print("Sending ack: ", segmentAck.to_string())

                    # Use the unreliable sendChannel to send the ack packet
                    self.sendChannel.send(segmentAck)

            else:
                segmentAck = Segment()
                acknum = self.lastAck

                # Display response segment
                segmentAck.setAck(acknum)
                print("Sending ack: ", segmentAck.to_string())

                # Use the unreliable sendChannel to send the ack packet
                self.sendChannel.send(segmentAck)


    def countSegmentTimeouts(self):
        """Returns the amount of segment timeouts that occurred"""
        return self.timeouts
