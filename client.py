from bitcoin import network
from bitcoin.BitcoinProtocol import *
from bitcoin import messages as msg
import threading
import datetime
import gevent
import sys
import time
from geoip import open_database
from math import radians, cos, sin, asin, sqrt
import copy
import msgpack




class Transactions:
        def __init__(self, client, timeout=60):
                self.lock=threading.RLock()
                self.timeout = timeout
                self.transactions = {}
		self.client = client


        def add(self, key, value):
                with self.lock:
                        self.transactions[key]=value
			#thread=threading.Timer(self.timeout,self.expire, args=(key,))
			#thread.daemon=True
                        #thread.start()
			gevent.spawn_later(self.timeout,self.expire, key)

        def expire(self,key):
                with self.lock:
                        #print "Transaction " + str(key).encode('hex')+ " expired"
                        del self.transactions[key]
			self.client.hashes[key]='expired'
                        #print "Expiring", key.encode('hex'), "still have", len(self.transactions)
                        #print "TRANSACTIONS"+str(len(self.transactions))

        def get(self,key):
                return self.transactions[key]

        def __len__(self):
                return len(self.transactons)

        def __str__(self):
                with self.lock:
                        return str(self.transactions)

class Fellow:
	def __init__(self, ip, port, proximity=1000):
		self.ip=ip
		self.port=port
		self.db = open_database('data/GeoLite2-City.mmdb')
		(self.magnitude, self.lognitude)=self.findLocation(self.ip)
		print str((self.magnitude, self.lognitude))
		self.proximity = proximity	#in Km	
		self.suggestions=set()	
		

	def findLocation(self, ip):
		#with open_database('data/GeoLite2-City.mmdb') as db:
    		match = self.db.lookup(ip)
		if match is not None:
			return match.location	
		else:	
			print 'no match'
			return (None,None)

	def distance(self, lon1, lat1, lon2, lat2):
		"""
		Calculate the great circle distance between two points 
		on the earth (specified in decimal degrees)
		"""
		# convert decimal degrees to radians 
		lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
		# haversine formula 
		dlon = lon2 - lon1 
		dlat = lat2 - lat1 
		a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
		c = 2 * asin(sqrt(a)) 
		km = 6367 * c
		#print km
		return km

	def isCloseTo(self,peerIP):
		try:
			location=self.findLocation(peerIP)
			if location is not None:
				(magn,logn)=location
				if magn is not None and logn is not None:
					dist=self.distance(magn, logn, self.magnitude, self.lognitude)
					if dist<self.proximity:
						#print "is close " + str(dist)
						return True	
			else: 
				return False
		except (AttributeError, TypeError):
			traceback.print_exc()
			return False
		

	def __eq__(self, other):
    		try:
        		return self.ip==other[0]
    		except AttributeError:
        		return False		
		except TypeError:
        		return False
	
	def __hash__(self):
		return hash(self.ip)

	def __str__(self):
		 return '('+self.ip+','+str(self.port)+')'

class BTCClient:

	def __init__(self, client, knownPeers, suggest=False, timeout = 60, refresh = 10):
		self.client = client
		self.hashes = {}
		self.timeout = timeout
		self.transactions = Transactions(self, self.timeout)
		self.peers=set()
		self.knownPeers=knownPeers
		self.proximatePeers=set()
		self.pool = ('104.236.4.90',8334)
		self.offset=None
		self.refresh=refresh
		self.lock = threading.RLock()
		self.suggest=suggest
		self.connectToPeers()	
		if self.suggest:
			self.suggestPeers()
		else:
			self.reconnectToPeers()

	def connectToPeers(self):
		with self.lock:
			print "connecting to CDN nodes"
			for p in [self.pool] + [(h.ip, h.port) for h in self.knownPeers.values()]:
				if p not in self.client.connections:
					self.connect(p, force=True)
			
			if not self.suggest:
				print "discovering network nodes"		        
				newPeers = dnsBootstrap()
				print "connecting to network nodes"
				connected = set(self.client.connections.keys())
				new_peers = newPeers - connected
				for p in new_peers:
					self.connect(p)
				print "connected"

	def reconnectToPeers(self):
		gevent.spawn_later(self.refresh, self.reconnectToPeers)
		with self.lock:
			print "discovering network nodes"		        
			newPeers = dnsBootstrap()
			print "connecting to network nodes"
			connected = set(self.client.connections.keys())
			new_peers = newPeers - connected
			for p in new_peers:
				self.connect(p)
			print "connected"


        def connect(self, host, force=False):
                if len(self.client.connections) > 100 and not force:
                        print "Refusing to open new connection to %s:%d" % host
                        return
                self.client.connect(host)
		self.peers.add(host)
                self.send_metadata('conn_up', host[0], '')
                #print host

        def suggestPeers(self):
		gevent.spawn_later(self.refresh, self.suggestPeers)
		with self.lock:
			print "discovering network nodes"
			newPeers = dnsBootstrap()
			print "finding suggestions"
			for fellow in self.knownPeers.values():
				for peer in newPeers:
					if fellow.isCloseTo(peer[0]):
						fellow.suggestions.add(peer)
				print str(len(fellow.suggestions))+" suggestions found for peer" +fellow.ip
			for c in self.client.connections.values():
				if c.connected:
					if c.host[0] in self.knownPeers:
						suggestionPct=SuggestionPacket()
						suggestionPct.suggestions = self.knownPeers[c.host[0]].suggestions
						c.send('sgst',suggestionPct)
						print "PROXIMATE PEER SUGGESTED"
	
	def handle_inv(self, connection, inv):
		pass

	def handle_tx(self, connection, tx):
		pass 

	def verify_transaction(self):
		pass

	def handle_getdata(self, connection, getdata):
		pass

	def handle_suggestion(self, connection, suggestion):
		print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@ suggestion message recieved @@@@@@@@@@@@@@@@@@@@@@@'
		for peer in suggestion.suggestions:
			if not peer in self.peers:
				self.connect(peer)
				print "Connecting to peer "+peer[0]
				self.peers.add(peer)
				self.proximatePeers.add(peer)
		#print self.peers
		
		

	def handle_time(self, connection, timeMsg):
		print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@ synchronization message recieved from '+str(connection.host)+' @@@@@@@@@@@@@@@@@@@@@@@'
		with self.lock:
			now=time.time()
			self.offset=timeMsg.time-now
			#print str(self.offset)
			connection.send('time',timeMsg)
	
	def send_metadata(self, msgtype, peerIp, tx_hash):
		metaPct = MetaPacket()
		metaPct.ip = peerIp
		metaPct.offset = self.offset
		metaPct.msgtype = msgtype
		metaPct.tx_hash = tx_hash
		metaPct.peers_no = len(self.client.connections)
		metaPct.arrival = time.time()
		#print self.client.connections.keys()
		if self.pool in self.client.connections:
			connection = self.client.connections[self.pool]
			if connection.connected:
				connection.send('meta',metaPct)
		else:
				pass

class normalBTCClient(BTCClient):

	def __init__(self, client, knownPeers, suggest=False, timeout = 60, refresh=10):
		BTCClient.__init__(self, client, knownPeers, suggest, timeout, refresh)
	
	def handle_inv(self, connection, inv):
		with self.lock:
			print("+++++++++++++++++++++++ inv message received ++++++++++++++++++++++++")
			#print("Invitation for %d transactions"%len(inv.hashes))
			req  = []
			for item in inv.hashes:
				self.send_metadata('inv', connection.host[0], item[1])
				if item[1] in self.hashes:
					pass			
					#print "\tTransaction with hashtag "+item[1].encode('hex')+" already known"
				else:
					#print "\tNew transaction with hashtag: "+item[1].encode('hex')
					self.hashes[item[1]]='pending'
					req.append(item)
				
	#		print ("Transactions known so far: %d"% len(self.hashes))
		
		if len(req)>0:
                	getDataPct = msg.GetDataPacket()
			getDataPct.hashes=req
			connection.send('getdata',getDataPct)

	def handle_tx(self, connection, tx):
                print("------------------------ tx packet recieved form "+str(connection.host) +" -----------------------------")
                #print "\tstore transaction with hash = "+tx.hash().encode('hex')
		self.send_metadata('tx', connection.host[0], tx.hash())
		with self.lock:
                	self.transactions.add(tx.hash(),tx)
                	self.hashes[tx.hash()]='recieved'
                invPct = msg.InvPacket()
                invPct.hashes.append([1,tx.hash()])
#               print(self.client.connections.values())
                for c in self.client.connections.values():
                        if c.connected:
                                c.send('inv',invPct)
                #print "transactions announced"

	def handle_getdata(self, connection, getdata):
                print "*********************** getData packet recieved ***************************"
                for item in getdata.hashes:
			self.send_metadata('getdata', connection.host[0], item[1])
                        if item[1] in self.transactions.transactions:
                                #print("\tForwarding transaction with hashtag "+item[1].encode('hex'))
                                txPct = self.transactions.get(item[1])
                                connection.send('tx',txPct)
                        else:
                                #print ("\tTransaction with hashtag "+item[1].encode('hex')+" expired")
				pass

class fastBTCClient(BTCClient):
		
	def __init__(self, client, knownPeers, suggest=False, timeout=60, refresh=10):
		BTCClient.__init__(self, client, knownPeers, suggest, timeout, refresh)
		self.pendingTransactions = {}

	def handle_inv(self, connection, inv):
		with self.lock:
			#print("+++++++++++++++++++++++ inv message received ++++++++++++++++++++++++")
			#print("Invitation for %d transactions"%len(inv.hashes))
			req  = []
			invPct = msg.InvPacket()

			for item in inv.hashes:
				self.send_metadata('inv', connection.host[0], item[1])
				if item[1] in self.hashes:
					pass			
					#print "\tTransaction with hashtag "+item[1].encode('hex')+" ALREADY KNOWN"
				else:
					#print "\tNew transaction with hashtag: "+item[1].encode('hex')
					self.hashes[item[1]]='pending'
					req.append(item)
		       			invPct.hashes.append(item)


		if len(req)>0:
                	getDataPct = msg.GetDataPacket()
                	getDataPct.hashes=req
                	connection.send('getdata',getDataPct)

	        	for c in self.client.connections.values():
                		if c.connected:
                        		if c.host[0] in self.knownPeers:
                                		c.send('inv',invPct)
                                        	#print "\ttransaction "+item[1].encode('hex')+ " announced to fast peer "+c.host[0]

	def handle_tx(self, connection, tx):
               	#print("------------------------ tx packet recieved form "+str(connection.host)+" -----------------------------")
                #print "\tstore transaction with hash = "+tx.hash().encode('hex')
		self.send_metadata('tx', connection.host[0], tx.hash())
		with self.lock:
                	self.transactions.add(tx.hash(),tx)
                	self.hashes[tx.hash()]='recieved'
                invPct = msg.InvPacket()
                invPct.hashes.append([1,tx.hash()])
#               print(self.client.connections.values())
                for c in self.client.connections.values():
                        if c.connected:
				if not(c.host[0] in self.knownPeers):
                                	c.send('inv',invPct)
					#print "\ttransaction "+tx.hash().encode('hex')+ " announced to peer "+c.host[0]
		if tx.hash() in self.pendingTransactions:
			with self.lock:
				if tx.hash() in self.transactions.transactions:
					for peer in self.pendingTransactions[tx.hash()]:	
						connection = self.client.connections[peer]
                                		txPct = self.transactions.get(tx.hash())
						if connection.connected:
                                			connection.send('tx',txPct)
							#print("\tForwarding pending transaction with hashtag "+tx.hash().encode('hex')+" to fast peer "+c.host[0])

                        	else:
                                	#print ("\tTransaction with hashtag "+item[1].encode('hex')+" expired")
					pass
				del self.pendingTransactions[tx.hash()]
				#print len(self.pendingTransactions)

	def handle_getdata(self, connection, getdata):
                #print "*********************** getData packet recieved from "+str(connection.host)+" ***************************"
		with self.lock:
		        for item in getdata.hashes:
				self.send_metadata('getdata', connection.host[0], item[1])
		                if item[1] in self.transactions.transactions:
		                        #print("\tForwarding transaction with hashtag "+item[1].encode('hex'))
		                        txPct = self.transactions.get(item[1])
		                        connection.send('tx',txPct)
		                else:
					if connection.host[0] in self.knownPeers:
						if self.hashes[item[1]]=='pending':
							if item[1] in self.pendingTransactions:
								self.pendingTransactions[item[1]].add(connection.host)
							else:
								self.pendingTransactions[item[1]]=set()
								self.pendingTransactions[item[1]].add(connection.host)
							#print "PENDING TRANSACTIONS"+ str(len(self.pendingTransactions))
						else:
		                        		#print ("\tTransaction with hashtag "+item[1].encode('hex')+" expired")
							pass
						
				
def startClient(client, btc):
        client.register_handler('inv', btc.handle_inv)
        client.register_handler('tx', btc.handle_tx)
        client.register_handler('getdata', btc.handle_getdata)
	client.register_handler('time', btc.handle_time)
	client.register_handler('sgst', btc.handle_suggestion)
        network.ClientBehavior(client)
        print btc.peers	
	client.listen()
        client.run_forever()



def start(argv):
	print "Starting..."
	client = network.GeventNetworkClient()
	if len(argv)<2:
		knownPeers={}
		suggest=False
	elif len(argv)<3:
		knownPeers={x:Fellow(x,8333) for x in argv[1].split(' ')}
		suggest=False
	else:
		knownPeers={x:Fellow(x,8333) for x in argv[1].split(' ')}
		if argv[2]=='suggest':
			suggest=True
		else:	
			print "Wrong Input"
			sys.exit(1)
		
	if argv[0]=='normal':
		btc = normalBTCClient(client,knownPeers,suggest,60,30)
	elif argv[0]=='fast':
		btc = fastBTCClient(client,knownPeers,suggest,60,30)
	else:
		print "Wrong Input"
		sys.exit(1)
	
	#results = Results(btc)
	#results.start()
	startClient(client, btc)
	#threads=[gevent.spawn(startClient(client, btc)), gevent.spawn(results.start())]
        #gevent.joinall(threads)
	


if __name__=="__main__":
	start(sys.argv[1:])
	#application = web.application(urls, globals()).wsgifunc()
        #print('Serving on 8088...')
        #threads=[gevent.spawn(start(sys.argv[1:])), gevent.spawn(WSGIServer(('', 8088), application).serve_forever())]
        #gevent.joinall(threads)
