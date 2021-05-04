from segment import Segment


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
        # Add items as needed
        self.nextSeqNum = 0
        self.lastAck = 0
        self.dataReceived = ''

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
        return self.dataReceived

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

        while len(self.sendChannel.sendQueue) < self.FLOW_CONTROL_WIN_SIZE % self.DATA_LENGTH:
            segmentSend = Segment()

            # ############################################################################################################ #
            print('processSend(): Complete this...')

            # You should pipeline segments to fit the flow-control window
            # The flow-control window is the constant RDTLayer.FLOW_CONTROL_WIN_SIZE
            # The maximum data that you can send in a segment is RDTLayer.DATA_LENGTH
            # These constants are given in # characters

            # Somewhere in here you will be creating data segments to send.
            # The data is just part of the entire string that you are trying to send.
            # The seqnum is the sequence number for the segment (in character number, not bytes)

            if self.dataToSend == '':  # if no data because server, break from loop
                print("I am the server, I don't send data.")
                break

            seqnum = self.nextSeqNum
            data = self.dataToSend[self.nextSeqNum:self.nextSeqNum+self.DATA_LENGTH]

            # ############################################################################################################ #
            # Display sending segment
            segmentSend.setData(seqnum,data)
            print("Sending segment: ", segmentSend.to_string())
            self.nextSeqNum += 4
            # Use the unreliable sendChannel to send the segment
            self.sendChannel.send(segmentSend)


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

        if not listIncomingSegments:  # if received nothing, return
            return

        data = [x for x in listIncomingSegments if x.seqnum != -1]

        data.sort(key=lambda x: x.seqnum)  # sort segments in order by seqnum

        for seg in data:
            print("Received Segment:", seg.payload)
            self.dataReceived = self.dataReceived + seg.payload

        # ############################################################################################################ #
        # How do you respond to what you have received?
        # How can you tell data segments apart from ack segments?
        print('processReceive(): Complete this...')

        # Somewhere in here you will be setting the contents of the ack segments to send.
        # The goal is to employ cumulative ack, just like TCP does...

        for x in data:
            segmentAck = Segment()  # Segment acknowledging packet(s) received
            acknum = x.seqnum + 4

            # ######################################################################################################
            # Display response segment
            segmentAck.setAck(acknum)
            print("Sending ack: ", segmentAck.to_string())

            # Use the unreliable sendChannel to send the ack packet
            self.sendChannel.send(segmentAck)

    def countSegmentTimeouts(self):
        # returns the number of segment timeouts
        return 0
