from metabitcoin import network
from metabitcoin.BitcoinProtocol import *
from metabitcoin import messages as msg
import threading
import datetime
import time
from client import Fellow
import sys
import xml.etree.cElementTree as ET
import csv
import gevent


class MetaClient:
	def __init__(self, client, peers):
		self.peers = peers
		self.client = client
		#for peer in peers.values():
                #	self.client.connect((peer.ip, peer.port))
		self.lock = threading.Lock()
		self.RTT={}
		
		with open('log.csv', 'w') as csvfile:
    			fieldnames = ['timestamp', 'RTT','arrival_offset','src_ip','message_type','peer_ip','tx_hash','peers_no','arrival']
    			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			writer.writeheader()
		self.synchronize()
		#self.sync_thread=threading.Thread(target=self.synchronize)
		#self.sync_thread.daemon=True
		#self.sync_thread.start()
	


	def synchronize(self):		
                gevent.spawn_later(10, self.synchronize)
		print "SYNCHRONIZING"
		#with self.lock:
		for c in self.client.connections.values():
			print c.host
			if c.connected:
				print "synchronizing with peer" + c.host[0]
				timePct = msg.TimePacket()
				timePct.ip = c.host[0]
				timePct.time = time.time()
				c.send('time', timePct)


	def handle_time(self, connection, timePct):	
		self.RTT[timePct.ip]=(time.time()-timePct.time)
		print "time message recieved"
	
	def handle_meta(self, connection, meta):
		print "metadata message recieved"
		now=str(datetime.datetime.now())
		if connection.host[0] in self.RTT:
			rtt=str(self.RTT[connection.host[0]])
		else:
			rtt=str(0)
		off=str(meta.offset)
		src=connection.host[0]
		msg=meta.msgtype
		peer=meta.ip
		peers_no=str(meta.peers_no)
		tx=meta.tx_hash.encode('hex')
		arr=str(meta.arrival)
		with open('log.csv', 'a') as csvfile:
    			fieldnames = ['timestamp', 'RTT','arrival_offset','src_ip','message_type','peer_ip','tx_hash','peers_no','arrival']
    			writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
			print 'timestamp'+now+','+ 'RTT'+rtt+','+'arrival_offset'+off+','+'src_ip'+src+','+'message_type'+msg+','+'peer_ip'+peer+','+'tx_hash'+tx+'peers_no'+peers_no+'arrival'+arr
 			writer.writerow({'timestamp':now, 'RTT':rtt,'arrival_offset':off,'src_ip':src,'message_type':msg,'peer_ip':peer,'tx_hash':tx, 'peers_no':peers_no,'arrival':arr})



		
		
		


def startClient(client, mtc):
	client.register_handler('time', mtc.handle_time)
	client.register_handler('meta', mtc.handle_meta)
        network.ClientBehavior(client)
	client.listen()
	print "Starting..."
        client.run_forever()
	



def start(argv):
	client = network.GeventNetworkClient()
	if len(argv)<1:
		peers={}
	else:
		peers={x:Fellow(x,8333) for x in argv[0].split(' ')}
	mtc = MetaClient(client,peers)
	
	#results = Results(btc)
	#results.start()
	startClient(client, mtc)
	print 'ok'
	#threads=[gevent.spawn(startClient(client, btc)), gevent.spawn(results.start())]
        #gevent.joinall(threads)
	


if __name__=="__main__":
	start(sys.argv[1:])
	#application = web.application(urls, globals()).wsgifunc()
        #print('Serving on 8088...')
        #threads=[gevent.spawn(start(sys.argv[1:])), gevent.spawn(WSGIServer(('', 8088), application).serve_forever())]
        #gevent.joinall(threads)
