from idigi_ws_api import Api, RestResource
from threading import Thread
import socket, ssl, pprint, struct, time
import json
import select

# Push Opcodes.
CONNECTION_REQUEST = 0x01
CONNECTION_RESPONSE = 0x02
PUBLISH_MESSAGE = 0x03
PUBLISH_MESSAGE_RECEIVED = 0x04

# Possible Responses from iDigi with respect to Push.
STATUS_OK = 200
STATUS_UNAUTHORIZED = 403
STATUS_BAD_REQUEST = 400

# Ports to Connect on for Push.
PUSH_OPEN_PORT = 3200
PUSH_SECURE_PORT = 3201

class PushException(Exception):
    """
    Indicates an issue interacting with iDigi Push Functionality.
    """
    pass

class PushSession(object):
    """
    A PushSession is responsible for establishing a socket connection
    with iDigi to receive events generated by Devices connected to 
    iDigi.
    """
    
    def __init__(self, callback, monitor, client):
        """
        Creates a PushSession for use with interacting with iDigi's
        Push Functionality.
        
        Arguments:
        callback -- The callback function to invoke when data is received.  
                    Must have 1 required parameter that will contain the
                    payload.
        monitor -- A RestResource Monitor instance.  This is used for 
                    determining monitor id, if compression is used,
                    what format to expect data in, etc.
        client -- The client object this session is derived from.
        """
        self.callback = callback
        self.monitor = monitor
        self.client = client
        self.socket = None
        
    def send_connection_request(self):
        """
        Sends a ConnectionRequest to the iDigi server using the credentials
        established with the id of the monitor as defined in the monitor 
        member.
        """
        try:
            # Send connection request and perform a receive to ensure
            # request is authenticated.
            # Protocol Version = 1.
            payload  = struct.pack('!H', 0x01)
            # Username Length.
            payload += struct.pack('!H', len(self.client.username))
            # Username.
            payload += self.client.username
            # Password Length.
            payload += struct.pack('!H', len(self.client.password))
            # Password.
            payload += self.client.password
            # Monitor ID.
            payload += struct.pack('!L', int(self.monitor.monId))

            # Header 6 Bytes : Type [2 bytes] & Length [4 Bytes]
            # ConnectionRequest is Type 0x01.
            data = struct.pack("!HL", CONNECTION_REQUEST, len(payload))

            # The full payload.
            data += payload

            # Send Connection Request.
            self.socket.send(data)

            # Set a 10 second blocking on recv, if we don't get any data
            # within 10 seconds, timeout which will throw an exception.
            self.socket.settimeout(10)

            # Should receive 10 bytes with ConnectionResponse.
            response = self.socket.recv(10)

            if len(response) != 10:
                raise PushException("Length of Connection Request Response \
(%d) is not 10." % len(response))

            # Type
            response_type = int(struct.unpack("!H", response[0:2])[0])
            if response_type != CONNECTION_RESPONSE:
                raise PushException("Connection Response Type (%d) is not \
ConnectionResponse Type (%d)." % (response_type, CONNECTION_RESPONSE))

            status_code = struct.unpack("!H", response[6:8])[0]
            if status_code != STATUS_OK:
                raise PushException("Connection Response Status Code (%d) is not \
STATUS_OK (%d)." % STATUS_OK)

            # Make socket blocking.
            self.socket.settimeout(0)
        except Exception, e:
            self.socket.close()
            self.socket = None
            raise e

    def start(self):
        """
        Creates a TCP connection to the iDigi Server and sends a 
        ConnectionRequest message.
        """
        if self.socket is not None:
            raise Exception("Socket already established for %s." % self)
        
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.client.hostname, PUSH_OPEN_PORT))
        except Exception, e:
            self.socket.close()
            self.socket = None
            raise e
        
        self.send_connection_request()
            
    def stop(self):
        """
        Closes the socket associated with this session and puts Session 
        into a state such that it can be re-established later.
        """
        self.socket.close()
        self.socket = None

class SecurePushSession(PushSession):
    """
    SecurePushSession extends PushSession by wrapping the socket connection
    in SSL.  It expects the certificate to match any of those in the passed
    in ca_certs member file.
    """
    
    def __init__(self, callback, monitor, client, ca_certs=None):
        """
        Creates a PushSession wrapped in SSL for use with interacting with 
        iDigi's Push Functionality.
        
        Arguments:
        callback -- The callback function to invoke when data is received.  
                    Must have 1 required parameter that will contain the
                    payload.
        monitor -- A RestResource Monitor instance.  This is used for 
                    determining monitor id, if compression is used,
                    what format to expect data in, etc.
        client -- The client object this session is derived from.
        
        Keyword Arguments:
        ca_certs -- Path to a file containing Certificates.  If not None,
        iDigi Server must present a certificate present in the ca_certs
        file.
        """
        PushSession.__init__(self, callback, monitor, client)
        self.ca_certs = ca_certs
    
    def start(self):
        """
        Creates a SSL connection to the iDigi Server and sends a 
        ConnectionRequest message.
        """
        if self.socket is not None:
            raise Exception("Socket already established for %s." % self)
        
        try:
            # Create socket, wrap in SSL and connect.
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Validate that certificate server uses matches what we expect.
            if self.ca_certs is not None:
                self.socket = ssl.wrap_socket(self.socket, 
                                                cert_reqs=ssl.CERT_REQUIRED, 
                                                ca_certs=self.ca_certs)
            else:
                self.socket = ssl.wrap_socket(self.socket)

            # TODO (possibly) for some reason python can't parse the peer
            # cert.  It would be really nice to assert that the hostname
            # matches what we expect.
            self.socket.connect((self.client.hostname, PUSH_SECURE_PORT))

        except Exception, e:
            self.socket.close()
            self.socket = None
            raise e
            
        self.send_connection_request()

class PushClient(object):
    
    def __init__(self, username, password, hostname='developer.idigi.com', 
                secure=True, ca_certs=None):
        """
        Creates a Push Client for use in creating monitors and creating sessions for them.
        
        Arguments:
        username -- Username of user in iDigi to authenticate with.
        password -- Password of user in iDigi to authenticate with.
        
        Keyword Arguments:
        hostname -- Hostname of iDigi server to connect to.
        secure -- Whether or not to create a secure SSL wrapped session.
        ca_certs -- Path to a file containing Certificates.  If not None,
        iDigi Server must present a certificate present in the ca_certs
        file if 'secure' is specified to True.
        """
        self.api      = Api(username, password, hostname)
        self.hostname = hostname
        self.username = username
        self.password = password
        self.secure   = secure
        self.ca_certs = ca_certs
        
        # A dict mapping Sockets to their PushSessions
        self.sessions    = {}
        self.__io_thread = None
        self.closed      = False

    def create_monitor(self, topics, batch_size=1, batch_duration=0, 
        compression='none', format_type='xml'):
        """
        Creates a Monitor instance in iDigi for a given list of topics.
        
        Arguments:
        topics -- a string list of topics (i.e. ['DeviceCore[U]', 'FileDataCore']).
        
        Keyword Arguments:
        batch_size -- How many Msgs received before sending data.
        batch_duration -- How long to wait before sending batch if it does not exceed batch_size.
        compression -- Compression value (i.e. 'zlib').
        format_type -- What format server should send data in (i.e. 'xml' or 'json').
        
        Returns a Monitor RestResource object.
        """
        
        # Create RestResource and POST it.
        monitor = RestResource.create('Monitor', monTopic=','.join(topics), 
                                        monBatchSize=str(batch_size),
                                        monBatchDuration=str(batch_duration),
                                        monFormatType=format_type,
                                        monTransportType='tcp',
                                        monCompression=compression)
        location = self.api.post(monitor)
        
        # Perform a GET by Id to get all Monitor data.
        monitor = self.api.get_first(location)
        # Set the location so it can be used for future reference.
        monitor.location = location
        return monitor
    
    def delete_monitor(self, monitor):
        """
        Attempts to Delete a Monitor from iDigi.  Throws exception if 
        Monitor does not exist.
        
        Arguments:
        monitor -- RestResource representing the monitor to delete.
        """
        self.api.delete(monitor)
        
    def get_monitor(self, topics):
        """
        Attempts to find a Monitor in iDigi that matches the input list of topics.
        
        Arguments:
        topics -- a string list of topics (i.e. ['DeviceCore[U]', 'FileDataCore']).
        
        Returns a RestResource Monitor instance if match found, otherwise None.
        """
        # Query for Monitor conditionally by monTopic.
        monitor = self.api.get_first('Monitor', condition="monTopic='%s'" % ','.join(topics))
        if monitor is not None:
            monitor.location = 'Monitor/%s' % monitor.monId
        return monitor
        
    
    def __select(self):
        """
        While the client is not marked as closed, performs a socket select
        on all PushSession sockets.  If any data is received, parses and
        forwards it on to the callback function.  If the callback is 
        successful, a PublishMessageReceived message is sent.
        """
        try:
            while not self.closed:
                try:
                    # TODO if any of the descriptors become bad
                    # (This can happen if session is stopped) remove
                    # it from sessions.
                    inputready, outputready, exceptready =\
                        select.select(self.sessions.keys(), [], [], .1)
                    for sock in inputready:
                        session = self.sessions[sock]
                        sck = session.socket
                        # 1.6mb payload limit (not enforced)
                        data = sck.recv(0x1000000)
                        # TODO assert minimum length, parse type, factor compression
                        if len(data) < 12:
                            # TODO: This is bad data, parse it and throw error
                            pass
                        response_type = struct.unpack('!H', data[0:2])[0]
                        aggregate_count = struct.unpack('!H', data[6:8])[0]
                        block_id = struct.unpack('!H', data[8:10])[0]
                        compression = struct.unpack('!B', data[10:11])[0]
                        format = struct.unpack('!B', data[11:12])[0]
                        payload_size = struct.unpack('!i', data[12:16])
                        payload = data[16:]
                        # TODO, throw in non-blocking event queue
                        if session.callback(payload):
                            # Send a Successful PublishMessageReceived with the 
                            # block id sent in request
                            response_message = struct.pack('!HHH', 
                                                PUBLISH_MESSAGE_RECEIVED, 
                                                block_id, 200)
                            sck.send(response_message)
                            
                            
                except Exception, ex:
                    print ex
                    pass # Raises exception if any descriptors are bad
                    # which is fine.
        finally:
            for session in self.sessions.values():
                session.stop()
                
    def create_session(self, callback, monitor=None, monitor_id=None):
        """
        Creates and Returns a PushSession instance based on the input monitor
        and callback.  When data is received, callback will be invoked.
        If neither monitor or monitor_id are specified, throws an Exception.
        
        Arguments:
        callback -- Callback function to call when PublishMessage messages are
            received. Expects 1 argument which will contain the payload of the 
            pushed message.  Additionally, expects function to return True if 
            callback was able to process the message, False or None otherwise.
        
        Keyword Arguments:
        monitor -- Monitor RestResource instance that will be registered on.
        monitor_id -- The id of the Monitor as id knows it, will be queried 
            to understand parameters of the monitor.
        """
        if monitor is None and monitor_id is None:
            raise PushException('Either monitor or monitor_id must be provided.')
            
        if monitor_id is not None:
            location = 'Monitor/%s' % monitor_id
            monitor = self.api.get_first(location)
            monitor.location = location

        session = SecurePushSession(callback, monitor, self, self.ca_certs) \
            if self.secure else PushSession(callback, monitor, self)

        session.start()
        self.sessions[session.socket.fileno()] = session
        
        # This is the first session, start the io_thread
        if self.__io_thread is None:
            self.__io_thread = Thread(target=self.__select)
            self.__io_thread.start()
        return session

    def stop_all(self):
        """
        Stops all session activity.  Blocks until io thread dies.
        """
        if self.__io_thread is not None:
            self.closed = True
            
            while self.__io_thread.is_alive():
                time.sleep(1)

def json_cb(data):
    """
    Sample callback, parses data as json and pretty prints it.
    Returns True is json is valid, False otherwise.
    
    Arguments:
    data -- The payload of the PublishMessage.
    """
    try:
        json_data = json.loads(data)
        print "Data Received: %s" % (json.dumps(json_data, sort_keys=True, indent=4))
        return True
    except Exception, e:
        print e
    
    return False
        
if __name__ == "__main__":
    client = PushClient('satest_user', 'sa!test', hostname='devtest.idigi.com',
                        secure=True, ca_certs='idigi.pem')
    topics = [ 'DeviceCore' ]
    monitor = client.get_monitor(topics)
    if monitor is None:
        monitor = client.create_monitor(topics, format_type='json')
    try:
        session = client.create_session(json_cb, monitor)
        while True:
            time.sleep(3.14)
    finally:
        client.stop_all()
        client.delete_monitor(monitor)
        
