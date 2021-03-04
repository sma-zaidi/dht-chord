import os, sys, threading, time
import socket, pickle
from hash import Hash

# CONSTANTS
m = 8 # no. of hash bits
KEY_SPACE = 2**m

STABILIZE_DELAY = 20 # gap between each stabilize iteration

PING_MAX_RETRIES = 3
PING_RETRY_DELAY = 20 # how long to wait before retry after ping goes unanswered

r = 2 # no. of successors and predecessors to keep track of

# message types
PING = 0
REQUEST_SUCCESSOR = 1 # response contains successor
REQUEST_PREDECESSOR = 2 # unused
REQUEST_FILES_LIST = 6
REQUEST_FILE = 7
REQUEST_FINGERS = 5
NOTIFY_SUCCESSOR = 3 # successor responds with their predecessor
NOTIFY_PREDECESSOR = 4
NOTIFY_LEAVE = 8
CONFIRM_LEAVE = 9
REQUEST_PUT = 10


# default node address
# can be passed as params
node_ip = '127.0.0.1'
node_port = 1111

# entry point into the network
entry_port = None

files = ""


# cmdline format: python chord.py node_port entry_port
if len(sys.argv) == 2:
    node_port = int(sys.argv[1])
elif len(sys.argv) >= 3:
    node_port = int(sys.argv[1])
    entry_port = int(sys.argv[2])

class ChordNode():
	def __init__(self, ip, port):
		self.active = False

		self.key = Hash(port)
		print("==========================")
		print("Initializing Chord node...")
		print("Port:", port)
		print("Node ID:", self.key)
		print("==========================")

		self.ip = ip
		self.port = port
		self.address = (ip, port)

		self.predecessor = self.port
		self.fingertable = []
		self.init_fingers()

		self.files = os.listdir()
		self.files.remove(os.path.basename(__file__))
		self.files.remove('__pycache__')
		self.files.remove('hash.py')
		self.transfer_in_progress = False

		self.s_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			self.s_socket.bind(self.address)
			print("\n[INFO] Node initialized on port", port)
		except socket.error:
			print(str(socket.error))
			sys.exit()

	def run(self): # init stabilize thread and start listening for connections

		stabThread = threading.Thread(target = self.stabilize)
		stabThread.daemon = True
		stabThread.start()

		self.s_socket.listen(2)

		while True:
			conn, addr = self.s_socket.accept()
			
			clientThread = threading.Thread(target = self.handler, args = (conn, addr))
			clientThread.daemon = True
			clientThread.start()

	def handler(self, conn, addr):
		while True:
			try:
				data = conn.recv(1024)
				if not data:
					break
				else:
					message = pickle.loads(data)

					if message['type'] == PING:
						conn.send(pickle.dumps("Pong!"))
						break

					elif message['type'] == REQUEST_SUCCESSOR:
						response = self.successor(message['key'])
						response = pickle.dumps(response)
						conn.send(response)
						break

					elif message['type'] == REQUEST_FILES_LIST:
						response = self.files
						response = pickle.dumps(response)
						conn.send(response)
						break

					elif message['type'] == REQUEST_FILE:
						print("[MSG] Node at " + str(message['source']) + " requests file with name: " + message['filename'])
						if message['filename'] in self.files:
							print("[INFO] Sending " + message['filename'] +  " to node at " + str(message['source']))

							size = os.path.getsize(message['filename']) # send the file size over first so client can somewhat track completion
							response = {'size': size}
							response = pickle.dumps(response)
							conn.send(response)

							with open(message['filename'], 'rb') as f:
								data = f.read(1024)
								while data:
									conn.send(data)
									data = f.read(1024)

							print("[UPDATE] Done sending!")
						else:
							print("No such file exists.")
							conn.send(pickle.dumps("ABSENT"))
						break

					elif message['type'] == REQUEST_FINGERS:
						response = []
						for finger in self.fingertable:
							response.append(finger['successor'])
						response = pickle.dumps(response)
						conn.send(response)
						break

					elif message['type'] == NOTIFY_SUCCESSOR:
						response = self.predecessor
						response = pickle.dumps(response)
						conn.send(response)
						self.predecessor = message['source']
						print(str(message['source']) + " is now my predecessor")
						break

					elif message['type'] == NOTIFY_PREDECESSOR:
						self.fingertable[0]['successor'] = message['source']
						print(str(message['source']) + " is now my successor")
						break

					elif message['type'] == NOTIFY_LEAVE:
						print("Predecessor is leaving!")
						print("Updating predecessor...")
						self.predecessor = message['predecessor']

						print("Requesting files from old predecessor")
						self.request_files(message['source'], True)

						print("Notifying new predecessor...")
						self.notify_predecessor(self.predecessor)

						print("All done.")
						response = {'type': CONFIRM_LEAVE}
						response = pickle.dumps(response)
						conn.send(response)
						break

					elif message['type'] == CONFIRM_LEAVE:
						self.transfer_in_progress = False
						print("OK TO LEAVE")

					elif message['type'] == REQUEST_PUT:
						print("Put request received.")
						self.request_file(message['filename'], message['source'])
						print("File successfully put:D")

					else: # ignore unrecognized requests
						break

					break
			except:
				break

		conn.close()

	def join(self, port):

		try:
			# request successor
			print("[INFO] Requesting successor...")
			self.fingertable[0]['successor'] = self.request_successor(port)
			print("[UPDATE] Received successor: " + str(self.fingertable[0]['successor']))

			# notify successor and request predecessor
			print("[INFO] Notifying successor and requesting predecessor...")
			self.predecessor = self.notify_successor(self.fingertable[0]['successor'])
			print("[UPDATE] Received predecessor: " + str(self.predecessor))

			# notify predecessor
			print("[INFO] Notifying predecessor...")
			self.notify_predecessor(self.predecessor)

			# request files from successor
			print("[INFO] Requesting files from successor...")
			self.request_files(self.fingertable[0]['successor'])
			print("[UPDATE] Receieved files.")

			print("[SUCCESS] Joined the network!")
			self.active = True

		except:
			print("[FAIL] Couldn't join the network")
			sys.exit()

	def request_files(self, port, download_all = False): # if download_all == true, download all files
		c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		c_socket.connect(('127.0.0.1', port))

		# request files
		message = {'type': REQUEST_FILES_LIST, 'source': self.port}
		message = pickle.dumps(message)
		c_socket.send(message)

		response = c_socket.recv(1024)
		response = pickle.loads(response)

		files = response
		print(response)
		files_to_get = []

		if download_all == False:
			for file in files:
				if self.successor(Hash(file)) == self.port:
					files_to_get.append(file)

		else:
			files_to_get = files

		if len(files_to_get) == 0:
			print("[INFO] No files to receive.")

		else:
			for file in files_to_get:
				print("[INFO] Requesting file:", file)
				self.request_file(file, port)

	def request_file(self, file, port):
		c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		c_socket.connect(('127.0.0.1', port))

		# request file
		if file in self.files:
			print("[ERR] File with the same name already exists.")
			return 0

		message = {'type': REQUEST_FILE, 'filename': file, 'source': self.port}
		message = pickle.dumps(message)
		c_socket.send(message)
		response = c_socket.recv(1024)
		response = pickle.loads(response)

		if str(response) == "ABSENT":
			print("[ERR] Requested file does not exist.")
		
		else:
			transfer_in_progress = True

			size = response['size']
			percent_received = 0
			old_percent_received = 0
			with open(file, 'wb') as f:
				response = c_socket.recv(1024)
					
				received = 1024.0

				print("Now receiving: " + file)
				while response:

					f.write(response)
					response = c_socket.recv(1024)
					received += 1024.0
					old_percent_received = percent_received
					percent_received = round((received/size) * 100)
					if percent_received > 100:
						percent_received = 100
					if old_percent_received != percent_received:
						print("\r" + "[TRANSFER] Progress: " + str(percent_received) + "%", end='')

			self.files.append(file)
			print("\n")
					

	def request_successor(self, port):

		c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		c_socket.connect(('127.0.0.1', port))

		# request successor
		message = {'type': REQUEST_SUCCESSOR, 'source': self.port, 'key': self.key}
		message = pickle.dumps(message)
		c_socket.send(message)
		response = c_socket.recv(1024)
		return pickle.loads(response)

	def notify_successor(self, port): # and request predecessor
		c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		c_socket.connect(('127.0.0.1', port))

		# notify successor
		message = {'type': NOTIFY_SUCCESSOR, 'source': self.port}
		message = pickle.dumps(message)
		c_socket.send(message)
		response = c_socket.recv(1024)
		return pickle.loads(response)

	def notify_predecessor(self, port):
		c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		c_socket.connect(('127.0.0.1', port))

		# notify predecessor
		message = {'type': NOTIFY_PREDECESSOR, 'source': self.port}
		message = pickle.dumps(message)
		c_socket.send(message)

	def successor(self, key):

		if key == self.key:
			return self.port

		nearest_node = self.port
		for finger in self.fingertable:
			if Hash(finger['successor']) > self.key:
				if key > self.key and key <= Hash(finger['successor']):
					return finger['successor']
				else:
					nearest_node = finger['successor']
					continue
			else:
				if Hash(finger['successor']) == self.key:
					if self.fingertable[0]['successor'] == self.port:
						return self.port
					else:
						break
				if key > self.key or key <= Hash(finger['successor']):
					return finger['successor']
				else:
					nearest_node = finger['successor']
					continue

		if nearest_node == self.port:
			return fingertable[0]['successor']
		else:
			#forward to nearest_node
			return self.fwd_successor_request(key, nearest_node)

	def fwd_successor_request(self, key, nearest_node):
		try:
			c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			c_socket.connect(('127.0.0.1', nearest_node))
		except:
			return None

		# request successor
		message = {'type': REQUEST_SUCCESSOR, 'source': self.port, 'key': key}
		message = pickle.dumps(message)
		c_socket.send(message)
		
		response = c_socket.recv(1024)
		return pickle.loads(response)


	def init_fingers(self):
		for i in range(m):
			finger = { 'i': (2**i), 'key': ((self.key + 2**i) % KEY_SPACE), 'successor': self.port}
			self.fingertable.append(finger)

	def fix_fingers(self):
		if self.fingertable[0]['successor'] != self.port:
			for i in range(1, m):
				self.fingertable[i]['successor'] = self.successor(self.fingertable[i]['key'])

	def ping(self, port):
		try:
			c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			c_socket.connect(('127.0.0.1', port))

			message = {'type': PING, 'source': self.port}
			message = pickle.dumps(message)
			c_socket.send(message)

			response = c_socket.recv(1024)
			response = pickle.loads(response)
			return True

		except:
			return False



	def stabilize(self):
		while True:
			time.sleep(STABILIZE_DELAY)

			self.fix_fingers()

			# ping successor
			update = False
			if(self.fingertable[0]['successor'] != self.port):
				for i in range(PING_MAX_RETRIES):
					if(self.ping(self.fingertable[0]['successor'])):
						update = False
						break
					else:
						update = True
						print("[STABILIZE] No response from successor, retrying in " + str(PING_RETRY_DELAY) + " seconds.")
						time.sleep(PING_RETRY_DELAY)

				if update == True:
					print("[STABILIZE] Current successor is down, updating...")

	def get_file(self, file):
		filehash = Hash(file)
		print("Requested file name:", file)
		print("Requested file hash:", filehash)
		print("Locating file...")
		succ_of_filehash = self.successor(filehash)
		print("File should be on node:", Hash(succ_of_filehash))
		print("Requesting node for file...")
		self.request_file(file, succ_of_filehash)

	def put_file(self, file):
		if file in os.listdir():
			if file in self.files:
				print("[ERR] File already exists.")
				return

			self.files.append(file)

			target = self.successor(Hash(file))
			if target == self.port:
				print("[UPDATE] PUT request complete.")
				return

			c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			c_socket.connect(('127.0.0.1', target))

			message = {'type': REQUEST_PUT, 'source': self.port, 'filename': file}
			message = pickle.dumps(message)
			c_socket.send(message)

			print("[INFO] PUT request forwarded to the responsible node.")
		else:
			print("[ERR] Invalid file name for PUT request.")

	def stop(self):
		try:
			print("Quitting...")
			print("Preparing node for graceful exit.")

			if(self.fingertable[0]['successor'] == self.port):
				print("[UPDATE] OK to leave.")
				sys.exit()

			# inform successor, tell them your predecessor
			c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			c_socket.connect(('127.0.0.1', self.fingertable[0]['successor']))

			message = {'type': NOTIFY_LEAVE, 'source': self.port, 'predecessor': self.predecessor, 'files': self.files}
			message = pickle.dumps(message)
			c_socket.send(message)

			response = c_socket.recv(1024)

			print("[UPDATE] OK to leave.")

			sys.exit()

		except:
			sys.exit()
		

##################################
######   ############# # #  #    # 
######   ###DRIVER#### # #  #    #
######   ############# # #  #    #
##################################

node = ChordNode(node_ip, node_port)

if entry_port != None: # if an entry point is provided, connect to it, else start a new network
	node.join(entry_port)

nodeThread = threading.Thread(target = node.run) # run the node on a seperate thread, using the main thread for input
nodeThread.daemon = True
nodeThread.start()

while True: # this loop handles user input
	try:
		command = input()
		command = command.split()

		if(len(command) == 1):
			if command[0] == "info":
				print("Node details:\n\tAddress: \t" + str(node.address) + "\n\tKey: \t\t" + str(node.key))
				print("\tSuccessor: \t" + str(Hash(node.fingertable[0]['successor'])) + "\n\tPredecessor: \t" + str(Hash(node.predecessor)))
				print("\tFinger table: \ttype 'fingertable' to view finger table")
				print("\tFiles: \t\t" + str(len(node.files)) + "\n\t\t\tType 'files' for more details.")

			elif command[0] == "fingertable":
				print("Finger table: ")
				for finger in node.fingertable:
					print("\t" + str(finger))

			elif command[0] == "files":
				print("Files")
				for file in node.files:
					print("\t" + str(file) + " " + str(Hash(file)))

			elif command[0] == "quit" or command[0] == "q" or command[0] == "exit":
				print("")
				node.stop()
				print("STOPPED")

		elif(len(command)) >= 2:
			if command[0] == "getfile":
				node.get_file(command[1])

			elif command[0] == "putfile":
				node.put_file(command[1])

	except:
		node.stop()

