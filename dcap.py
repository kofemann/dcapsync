#!/usr/bin/env python

from urlparse import urlparse
import socket
import cStringIO
import struct
import time
import os 

VER_MAJ = 0
VER_MIN = 0

# protocol constants

END_OF_DATA =  0xffffffff
DCAP_WRITE = 1
DCAP_READ = 2
DCAP_CLOSE = 4
DATA = 8

def _merge_string(b):
	r = ''
	for s in b:
		r = r + ' ' + s
	return r

class Dcap:
	"""dCache Client Access Protocol DCAP"""

	def __init__(self, url, root='/'):
		u = urlparse(url)
		self.host = u.hostname
		self.port = u.port
		self.seq = 0
		self._connect()
		self._send_hello()

	def _connect(self):
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.connect((self.host, self.port))

	def _close(self):
		self.socket.close()

	def _rcv_control_msg(self):
		msg = ''
		while True:
			chunk = self.socket.recv(1)
			if chunk == '':
				raise RuntimeError("socket connection broken")
			if chunk == '\n':
				break
			msg = msg + chunk
		return msg

	def _send_control_msg(self, msg):
		sent = self.socket.sendall(msg + '\n')
		if sent == 0:
			raise RuntimeError("socket connection broken")
		self.seq += 1

	def _send_hello(self):
		hello = "%d 0 client hello %d %d %d %d" % (self.seq, VER_MAJ, VER_MIN, VER_MAJ, VER_MIN)
		self._send_control_msg(hello)
		reply = self._rcv_control_msg()

	def _send_bye(self):
		bye = "%d 0 client byebye" % (self.seq)
		self._send_control_msg(bye)
		reply = self._rcv_control_msg()

	def open_file(self, path, mode='r'):
		session = self.seq
		open_opemmand = "%d 0 client open dcap://%s:%d/%s %s localhost 1111 -passive" % \
			(self.seq, self.host, self.port, path, mode )
		self._send_control_msg(open_opemmand)
		reply = self._rcv_control_msg()
		host, port, chalange = self.parse_reply(reply)
		
		data_socket = self._init_data_connection(session, host, port, chalange)
		return DcapStream(data_socket, self)

	def parse_reply(self, reply):
		s = reply.split()
		if s[3] == 'failed':
			raise RuntimeError("failed to open file: " + _merge_string(s[5:]))
		return s[4], int(s[5]), s[6]

	def _init_data_connection(self, session, host, port, chalange):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host+'.desy.de', port))

		packer = struct.Struct('>II')

		s.send(packer.pack(session, len(chalange)))
		s.send(chalange)
		return s
		
	def rename(self, src, dest):
		rename_cmd = "%d 0 client rename dcap://%s:%d/%s %s" % \
			(self.seq, self.host, self.port, src, dest )
		self._send_control_msg(rename_cmd)
		reply = self._rcv_control_msg()

class DcapStream:

	def __init__(self, sock, dcap):
		self.socket = sock;
		self.dcap = dcap

	def _get_ack(self):
		unpacker = struct.Struct('>I')
		msg = self.socket.recv(unpacker.size)
		size = unpacker.unpack(msg)[0]
		msg = self.socket.recv(size)
		# do something here

	def _get_data(self):
		unpacker = struct.Struct('>II')
		msg = self.socket.recv(unpacker.size)

		data_unpacker = struct.Struct('>I')
		data = ''
		while True:
			data_header = self.socket.recv(data_unpacker.size)
			count = data_unpacker.unpack(data_header)[0]

			if count == END_OF_DATA:
				break
			data = data + (self.socket.recv(count))
		return data

	def read(self, count):
		packer = struct.Struct('>IIq')
		msg = packer.pack( 12, DCAP_READ, count)
		self.socket.sendall(msg)
		self._get_ack()
		return self._get_data()

	def close(self):
		packer = struct.Struct('>II')

		msg = packer.pack(4, DCAP_CLOSE)
		self.socket.sendall(msg)
		self._get_ack()
		reply = self.dcap._rcv_control_msg()
		self.socket.close()

	def send_file(self, src):

		statinfo = os.stat(src)
		packer = struct.Struct('>II')
		msg = packer.pack(4, DCAP_WRITE)
		self.socket.sendall(msg)
		self._get_ack()

		data_packer = struct.Struct('>III')
		data_header = data_packer.pack(4, DATA, statinfo.st_size)
		self.socket.sendall(data_header)

		with open(src,'r') as f:
			while True:
				data = f.read(256*1024)
				if len(data) == 0:
					break
				self.socket.sendall(data)

		data_packer = struct.Struct('>I')
		data_header = data_packer.pack(END_OF_DATA)
		self.socket.sendall(data_header)
		self._get_ack()

	def write(self, buf):
		packer = struct.Struct('>II')
		msg = packer.pack(4, DCAP_WRITE)
		self.socket.sendall(msg)
		self._get_ack()

		data_packer = struct.Struct('>III')
		data_header = data_packer.pack(4, DATA, len(buf))
		self.socket.sendall(data_header)
		self.socket.sendall(buf)
		data_packer = struct.Struct('>I')
		data_header = data_packer.pack(END_OF_DATA)
		self.socket.sendall(data_header)
		self._get_ack()

if __name__ == "__main__":
	dcap = Dcap("dcap://dcache-lab000.desy.de:22125")

	f = dcap.open_file('/exports/data/file114', 'w')

	f.send_file('/etc/profile')
	f.close()

	dcap._send_bye()
	dcap._close()
