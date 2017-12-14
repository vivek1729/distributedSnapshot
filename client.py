import socket
import sys
import json
import time
import random
import traceback
import logging
from threading import Thread

'''
Check that send happens only when account balance >=1
On first marker receive , process state save
Set flags for all  channels and on receive
if flag is set, save the state of channel
On receive a marker if flag is set for that channel
close the channel, reset flag to false.
Save channel state on marker receive

See if you can break out of infinite while loop or close the thread
after connections are complete.

channel_state = {
    '2' : {
    'flag': True|False
    'state': []
    },
    '3' : {
    'flag': True|False
    'state': []
    }
}

Problem:
1. When receiving two markers from two different processes in quick succession, 
the global variable is over-ridden, so the first marker is not being processed.
2. While saving state, how to handle other messages received on the same channel.
'''





#Global config variable declaration with sane defaults
TCP_IP = '127.0.0.1'
threads = []
clients = []
PROC_ID_MAPPING = {}
ACCOUNT_BALANCE = 1000
SAVING_STATE = False
MARKER_MODE = []
SAVED_STATE = None
channel_states = {}
REPLY_DELAY = 10
per_threshold = 100 #Randomly send message with 20% every 10 seconds.
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
logger= logging.getLogger(__name__)
client_id = ' '.join(sys.argv[1:])
with open('config.json') as data_file:    
    data = json.load(data_file)
    TCP_IP = data['tcp_ip']
    TCP_PORT = data[client_id]['tcp_port']
    REPLY_DELAY = data['reply_delay']
    REQUEST_DELAY = data['request_delay']
    TOTAL_CLIENTS = data['total_clients']
    PROC_ID_MAPPING = data['reverse_dict']
    per_threshold = data['per_threshold']


print client_id
print TOTAL_CLIENTS
print TCP_IP
print TCP_PORT
print PROC_ID_MAPPING
serverSocket.bind((TCP_IP, TCP_PORT))
#logger.info('Application Connected at host '+ str(host) +' and at port '+ str(port))
serverSocket.listen(10)
tcpClient = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
TOTAL_CLIENTS.remove(TCP_PORT)
print 'Total clients is '
print TOTAL_CLIENTS
for client in TOTAL_CLIENTS:
    key = PROC_ID_MAPPING[str(client)]['proc_id']
    channel_states[key] = {}
    channel_states[key]['flag'] = False
    channel_states[key]['state'] = []

print 'Initialized channel states'
print channel_states
send_channels = {}
receive_channels = {}
queue_send_markers = []
SNAPSHOT_DICT = {}
#initialize 
def startLoggin():
    global logger
    print 'I have started logging ! Check the logs !'
    log_file_name= client_id+ "_client.log"
    
    logger.setLevel(logging.DEBUG)
    
    #creating logger file
    handler=logging.FileHandler(log_file_name)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    #logging.basicConfig(filename=log_file_name,level=logging.DEBUG)
def create_snapshot_dict():
    SNAPSHOT_DICT[client_id] = {}
    for client in TOTAL_CLIENTS:
        proc_id = PROC_ID_MAPPING[str(client)]['proc_id']
        SNAPSHOT_DICT[proc_id] = {}
    for key in SNAPSHOT_DICT:
        channel_states = {}
        for client in TOTAL_CLIENTS:
            key1 = PROC_ID_MAPPING[str(client)]['proc_id']
            channel_states[key1] = {}
            channel_states[key1]['flag'] = False
            channel_states[key1]['state'] = []
        SNAPSHOT_DICT[key] = {'SAVED_STATEN':None,'SAVING_STATEN':False,'channel_states': channel_states}
    
    logger.info('Initialization is complete for snapshot dict')
    #print 'Initialization is complete for snapshot dict'
    logger.debug(SNAPSHOT_DICT)
    #print SNAPSHOT_DICT


class ClientThread(Thread):

    def __init__(self,ip,port,conn): 
        Thread.__init__(self) 
        self.ip = ip 
        self.port = port
        self.conn = conn
        logger.info("[+] New socket thread started for " + ip + ":" + str(port))
        #logger.info('New Client\'s thread started')

    def run(self): 
            self.receiveData();

    def print_total_val(self,initiator_id):
        total_val = SNAPSHOT_DICT[initiator_id]['SAVED_STATEN']
        for id in SNAPSHOT_DICT[initiator_id]['channel_states']:
            channel = SNAPSHOT_DICT[initiator_id]['channel_states'][id]
            total_val += sum(int(c['msg']) for c in channel['state'])
        print "Total Value of process after local snapshot : "+str(total_val)


    def reset_state(self,initiator_id):
        logger.info('Reset state called.') 
        #Reset process state
        SNAPSHOT_DICT[initiator_id]['SAVED_STATEN'] = None
        #Reset channel states
        for id in SNAPSHOT_DICT[initiator_id]['channel_states']:
            #Reset flag to false and state as empty
            SNAPSHOT_DICT[initiator_id]['channel_states'][id]['flag'] = False
            SNAPSHOT_DICT[initiator_id]['channel_states'][id]['state'] = []
        logger.info('Channel states and process state have been reset') 
            

    def receiveData(self):
    #print "Staring thread for client"
        #print "I am here too"
        #logger.info('I have entered message passing')
        while True:
            try:
                global ACCOUNT_BALANCE
                global SAVING_STATE
                global SAVED_STATE
                global MARKER_MODE
                received_data = self.conn.recv(2048)
                logger.debug( 'Data received from client, value '+received_data)
                if received_data != '':
                    received_data_Json=json.loads(received_data)
                    if(self.conn in receive_channels.values()) :
                       #logger.info(self.id+': an old client is sending message')
                       logger.info( 'Old client sending message')
                    else:
                        receive_channels[received_data_Json['id']]=self.conn
                        self.id = received_data_Json['id']
                        logger.debug( receive_channels)
                        logger.info( 'It is a new Client !! Yippee !! ' + received_data_Json['id'])
                    received_message = received_data_Json['msg']
                    request_type = received_data_Json['type']
                    if request_type == 'marker':
                        initiator_id = received_message
                        #Got the first marker from initiator
                        #Save state of process and save state of channel as empty
                        logger.info( '=======> Received a marker from '+self.id)

                        if SNAPSHOT_DICT[initiator_id]['SAVED_STATEN']:  #marker not first tym
                            logger.info( 'Saving STATE global var ')
                            logger.debug(SAVING_STATE)
                            #print SAVING_STATE
                            logger.info('State already saved')
                            #print 'State already saved'
                            SNAPSHOT_DICT[initiator_id]['channel_states'][self.id]['flag'] = False
                            is_complete = True
                            for id in SNAPSHOT_DICT[initiator_id]['channel_states']:
                                if SNAPSHOT_DICT[initiator_id]['channel_states'][id]['flag']:
                                    is_complete = False

                            if is_complete:
                                print '=============================='
                                print 'Snapshot initiated by : '+initiator_id
                                print 'Completed saving process state'
                                print SNAPSHOT_DICT[initiator_id]['SAVED_STATEN']
                                print 'Saved all channel states'
                                print SNAPSHOT_DICT[initiator_id]['channel_states']
                                print '=============================='
                                self.print_total_val(initiator_id)
                                self.reset_state(initiator_id)   
                        else:
                            #First marker received
                            #Enter marker mode
                            #Marker mode can be array [is marker mode or not, is initiator or not]
                            #if initiator is true
                            #Save process ID in marker mode array
                            #Do all the process of saving state, saving incoming channels and sending markers in sendData function
                            #print 'Saving current state'
                            logger.info('Saving current stat')
                            SNAPSHOT_DICT[initiator_id]['SAVED_STATEN'] = ACCOUNT_BALANCE
                            SAVING_STATE = True
                            #Add initiator id to queue. Initiator ID is in the msg key of data
                            queue_send_markers.append(received_message) #phase 1
                            #Set flags for all other states to be true so that messages can be saved
                            for id in SNAPSHOT_DICT[initiator_id]['channel_states']: #Phase 2
                                SNAPSHOT_DICT[initiator_id]['channel_states'][id]['flag'] = True #Phase 2

                            SNAPSHOT_DICT[initiator_id]['channel_states'][self.id]['flag'] = False #Phase 2
                            SNAPSHOT_DICT[initiator_id]['channel_states'][self.id]['state'] = [] #Phase 2
                            logger.info('Channel flags are also set now.')
                            #print 'Channel flags are also set now.'

                    elif request_type == 'transfer':
                        print 'Money received from '+received_data_Json['id']+' and amount '+received_message
                        #print 'Got some money from transfer : '+received_message
                        logger.debug('Received some data '+received_message+' from '+received_data_Json['id'])
                        #print 'Received some data '+received_message+' from '+received_data_Json['id']
                        ACCOUNT_BALANCE = ACCOUNT_BALANCE + int(received_message)
                        print 'Updated account balance '+str(ACCOUNT_BALANCE)
                        #print 'Updated account balance '+str(ACCOUNT_BALANCE)

                        #Loop over all initiator Ids.
                        for key in SNAPSHOT_DICT:
                            if SNAPSHOT_DICT[key]['channel_states'][self.id]['flag']:
                                SNAPSHOT_DICT[key]['channel_states'][self.id]['state'].append(received_data_Json)


            except Exception as e:
                #print e
                logger.info("Some Exception")
                #print "Some Exception"
                #traceback.print_exc()
                break;
                self.conn.close()
                serverSocket.close()



def listenNewClients():
    while True:
        try:
            logger.info('Waiting for connections...')
            #print 'Waiting for connections...'
            (conn, (ip,port)) = serverSocket.accept()
        

            newthread = ClientThread(ip,port,conn)
            threads.append(newthread)
            clients.append(conn)
            newthread.start()
           
            #conn.settimeout(60)
        except:
                logger.info('listenNewClients Exception')
                #print 'Some problem !'
                conn.close()
                serverSocket.close()
                break

def connectAsClient():
    logger.info('Gonna try and connect to following ports now..')
    #print 'Gonna try and connect to following ports now..'
    global TOTAL_CLIENTS
    logger.info(TOTAL_CLIENTS)
    #print TOTAL_CLIENTS
    while True:
        time.sleep(5)
        try:
            if len(TOTAL_CLIENTS) > 0:
                tcpClient = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                logger.info( 'Trying to connect to '+str(TOTAL_CLIENTS[0]))
                tcpClient.connect((TCP_IP, TOTAL_CLIENTS[0]))
                key = PROC_ID_MAPPING[str(TOTAL_CLIENTS[0])]['proc_id']
                logger.info('Key is '+key)
                send_channels[key] = tcpClient
                logger.debug( send_channels)
                logger.info('Length of send channels')
                logger.debug( len(send_channels))
                del TOTAL_CLIENTS[0]
                logger.info('Remaining clients are ')
                logger.debug(TOTAL_CLIENTS)
            else:
                logger.info('Have successfully connected to all clients!')
                break
        except:
            logger.info('Unable to connect')

def setup():
    #Initialize snapshot dictionary
    startLoggin()
    create_snapshot_dict()
    
    print 'You are in ! Send some Message!'
    sys.stdout.write('[Me] ')
    sys.stdout.flush()
    masterThread = Thread(target=listenNewClients)
    masterThread.start()
    connectThread = Thread(target=connectAsClient)
    connectThread.setDaemon(True)
    connectThread.start()
    sendThread = Thread(target=sendData)
    #sendThread.setDaemon(True)
    sendThread.start()
    takeSnapshot()

'''
For sending money as well as markers
'''
def sendData():
    global ACCOUNT_BALANCE
    global SAVING_STATE
    global SAVED_STATE
    global MARKER_MODE
    while True:
        try:
            logger.info('<==============>')
            logger.info('Saving state value for iteration...')
            logger.debug( SAVING_STATE)
            logger.info('<==============>')
            if len(queue_send_markers) > 0:
                #I have received marker or initiated snapshot, save state first
                '''
                SAVED_STATE = ACCOUNT_BALANCE

                #if not initiator save incoming channel state as empty and start listening on other incoming channels
                for id in channel_states:
                    channel_states[id]['flag'] = True

                #If not initiator, I will have marker source
                #save channel state as empty for the marker sender
                print MARKER_MODE
                if MARKER_MODE[0]:
                    channel_states[MARKER_MODE[1]]['flag'] = False
                    channel_states[MARKER_MODE[1]]['state'] = []
                '''
                #Loop over all processes in queue and send markers to all outgoing channels
                while len(queue_send_markers) > 0: #phase 1
                    initiator_id = queue_send_markers[0]
                    to_send = json.dumps({'id': client_id, 'type': 'marker','msg': initiator_id})
                    logger.info('Length of send channels is ')
                    logger.debug(len(send_channels))
                    #Sleep for some time before sending markers. This would allow it to receive messages
                    #in receiving channels before saving state.
                    for chan in send_channels:
                        print 'Sending marker to: '+chan
                        send_channels[chan].send(to_send)

                    logger.info('Process id: '+str(initiator_id)) #Phase 2
                    logger.debug('State saved for above  '+str(SNAPSHOT_DICT[initiator_id]['SAVED_STATEN'])) #Phase 2
                    logger.info('State of channels...')
                    logger.debug(SNAPSHOT_DICT[initiator_id]['channel_states'])#Phase 2

                    SAVING_STATE = False
                    #Remove first item from queue_send_markers
                    queue_send_markers.pop(0) #phase 1
                    time.sleep(0.5)
                time.sleep(REPLY_DELAY/2)
            else:
                logger.info('Why did it go to else if value of SAVING STATE is')
                logger.debug(SAVING_STATE)
                if random.randrange(0,100) <= per_threshold and ACCOUNT_BALANCE > 0 and len(TOTAL_CLIENTS) == 0:
                    if len(send_channels) > 0:
                        logger.info('Attempting to send some money now!')
                        money = random.randint(1,ACCOUNT_BALANCE)
                        ACCOUNT_BALANCE = ACCOUNT_BALANCE - money
                        index = random.randint(0,1)
                        reipient_id = send_channels.keys()[index]
                        reipient_conn = send_channels.values()[index]
                        to_send = json.dumps({'id': client_id, 'type': 'transfer','msg': str(money)})
                        reipient_conn.send(to_send)
                        logger.info('Money sent to '+reipient_id+' and amount '+str(money))
                        logger.info('Remaining balance is'+str(ACCOUNT_BALANCE))
                        print 'Money sent to '+reipient_id+' and amount '+str(money)
                        print 'Remaining balance is'+str(ACCOUNT_BALANCE)
                #time.sleep(10)
            #Sleep for a random time right now. To may be capture messages in channels
            #time.sleep(5+random.randint(0,5))
            time.sleep(REPLY_DELAY)
        except Exception as e:
            logger.info('Some error occured: ' + str(e))
    logger.info('It got out of the while loop? some prob??')

'''
Function to start taking snapshot
'''       
def takeSnapshot():
    #Send the connect request first.
    #welcome_message = json.dumps({'id': self.id, 'type': 'connect',
    #        'msg': 'hola', 'to': 'all'})
    logger.info('Length of clients connected')
    logger.debug(len(clients))
    global SAVING_STATE
    global MARKER_MODE
    global SAVED_STATE
    
    while 1==1:
        #time.sleep(1)
        msg = sys.stdin.readline()
        msg = msg[:-1]  # omitting the newline
        if msg == 'exit':
            break
        if msg != '':
            logger.info('Msg to be sent is ...'+msg)
            print 'Initiating snapshot'
            #to_send = json.dumps({'id': client_id, 'type': 'marker','msg': 'hola'})
            initiator_id = client_id #Phase 2
            SNAPSHOT_DICT[initiator_id]['SAVED_STATEN'] = ACCOUNT_BALANCE #Phase 2
            #SAVED_STATE = ACCOUNT_BALANCE
            SAVING_STATE = True
            queue_send_markers.append(initiator_id) #Phase 1
            #Set flags for all other states to be true so that messages can be saved
            for id in SNAPSHOT_DICT[initiator_id]['channel_states']: #Phase 2
                SNAPSHOT_DICT[initiator_id]['channel_states'][id]['flag'] = True #Phase 2
                
        sys.stdout.write('[Me] ')
        sys.stdout.flush()

setup()