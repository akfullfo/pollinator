# ________________________________________________________________________
#
#  Copyright (C) 2016 Andrew Fullford
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ________________________________________________________________________
#
 
import os, time, errno, socket, json, re, logging
from . import poll

def_connect_timeout = 30.0
def_input_size = 4096

def getparam(obj, name, default=None):
	"""
	Descend the object hierarchy to find a matching
	parameter and return the value.  The descent stops
	when an object has no parent attribute.
"""
	while obj:
		if hasattr(obj, 'params') and name in obj.params:
			return obj.params[name]
		elif hasattr(obj, 'parent'):
			obj = obj.parent
		else:
			break
	return default

_log_discard = logging.getLogger(__name__)
_log_discard.addHandler(logging.NullHandler())

class Error(Exception): pass

class Listener(object):
	"""
	A Listener is defined to handle connections for a listening socket.
	It will be registered in poll_set, an instance of poll.poll.  When a
	poll indicates input on a listen socket, a new instance of channel_class will
	be created to handle I/O on the incomming connection.  These channel_class
	instances will be added and removed as necessary from the poll_set.

	Parameters:

	  log		- A "logging" instance.
	  closeall	- If True (default), all listen sockets will be closed
	  		  when the context exits.  If False, only channels
			  are closed.
"""
	def __init__(self, sock, **params):
		self.params = params
		self.log = getparam(self, 'log', _log_discard)
		self.closeall = getparam(self, 'closeall', True)
		if not self.log:
			self.log = logging.getLogger(__name__)
			self.log.addHandler(logging.NullHandler())

		self._pset = getparam(self, 'poll_set')
		if self._pset is None:
			raise Error("poll_set parameter is required")
		if not isinstance(self._pset, poll.poll):
			raise Error("poll_set argument must be an instance of poll.poll")

		self._sock = sock
		self._pset.register(self, poll.POLLIN)

		self._channels = set()
		self._channel_class = getparam(self, 'channel', Channel)
		if not issubclass(self._channel_class, Channel):
			raise Error("channel_class argument must be a subclass of Channel")

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self._pset.unregister(self._sock)
		self.reset()

	def fileno(self):
		return self._sock.fileno()

	def handle(self, chan, mask):
		"""
		Perform operations as needed for all events returned by a poll() call.
		The list may include objects not managed by this channel_set.  These
		are just ignored.  The scope() method can be used to determine if an
		object is in the channel set.
	"""
		if chan == self:
			sock, client = self._sock.accept()
			self.log.info("New connection from %s", repr(client))
			self._channels.add(self._channel_class(sock, self))
		elif chan in self._channels:
			return chan.handle(chan, mask)
		else:
			self.log.info("Event out of context for this channel_set")
	
	def reset(self):
		"""
		Clear out all existing connections.  This should be
		called after any disruption in processing, eg when the
		parent restarts itself following an unexpected exception.
		It is safe to call at any time but note that existing
		connections are closed immediately without following the
		shutdown protocol.
	"""
		self.log.info("Reset all channels")
		for chann in self._channels:
			try: chann._close()
			except: pass
		self._channels = set()

	def call(self, method, *args, **params):
		"""
		Calls the named method on all recorded channels.
		"args" and "params" are passed straight through.
	"""
		for chann in list(self._channels):
			getattr(chann, method)(*args, **params)

class Pump(object):
	"""
	Maintains a queue of output messages and an input buffer containing data
	from messages that are as-yet incomplete.

	Subclassing is supported as long as the message size values are
	maintained accurately.  The subclass can be specified in a listener
	or a channel with the 'pump' param.
"""
	def __init__(self, sock, parent, **params):
		self._sock = sock
		self.parent = parent
		self.params = params
		self.log = getparam(self, 'log', _log_discard)
		self.connect_timeout = getparam(self, 'connect_timeout', def_connect_timeout)
		self.input_size = getparam(self, 'input_size', def_input_size)
		self.started = time.time()
		self.remote = None
		self.connection_lost = False
		self.transmitting = ''
		self.receiving = ''

	def __del__(self):
		self.connection_lost = True
		self.close()

	def fileno(self):
		return self._sock.fileno()

	def close(self):
		if self._sock:
			if self.connection_lost:
				self._sock.close()
				self._sock = None
			else:
				self._sock.shutdown(socket.SHUT_WR)
				self.connection_lost = True

	def connect(self):
		err = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
		if err == errno.EINPROGRESS:
			self.log.info("Connection in progress")
			return False
		try:
			self.remote = self._sock.getpeername()
			self.log.info("Now connected to %s", repr(self.remote))
			return True
		except socket.error as e:
			if e.errno == errno.ENOTCONN:
				self.log.info("Not connected")
				delta = time.time() - self.started
				self.connection_lost = True
				if self.connection_timeout > 0 and delta > self.connection_timeout:
					raise Error("Connection timed out after " + utils.deltafmt(delta))
			else:
				self.log.info("Connect failed -- %s", str(e))
				self.connection_lost = True
				raise e
		except:
			self.connection_lost = True
			raise

	def read(self):
		"""
		Read raw data from the connection.

		If data is read, it is appended to the receive buffer and True is
		returned.

		An empty read is interpretted as a closed connection, The socket is
		shut down and False is returned.
	"""
		if self.connection_lost:
			return
		if self.remote is None and not self.connect():
			return
		data = self._sock.recv(self.input_size)
		if data == '':
			self.connection_lost = True
			self._sock.shutdown(socket.SHUT_WR)
			return False
		self.receiving += data
		return True

	def write(self):
		"""
		Write raw data to the conenction.

		Data successfully written is removed from the transmit buffer.

		The function returns False if no more data is pending, otherwise
		True.  The caller will typically remove poll.POLLOUT from the
		pset for this channel and add it back when new data is queued.
	"""
		if self.connection_lost:
			return
		if self.remote is None and not self.connect():
			return
		if self.transmitting:
			cnt = self._sock.send(self.transmitting)
			if cnt == 0:
				self.log.warning("socket.send() delivered no data")
			else:
				self.transmitting = self.transmitting[cnt:]
		else:
			self.log.warning("called with no data pending")
		return len(self.transmitting) > 0


class Channel(object):
	"""

	Handles channel communication by encapsulating outbound messages and decasulating
	inbound messages.  Inbound messages are handled via a generator (see read()
	below.	A typical client might look like:

		pset = poll.poll()
		chan = channel.Channel(sock, None, poll_set=pset)
		chan.queue(question)
		while True:
			for c, mask in pset.poll():
				c.handle(mask)
				for answer in c.read():
					next_question = process(answer)
					if next_question:
						c.queue(next_question)
			if len(pset) == 0:
				break

	A channel is created to handle data obect transfers
	in both client and server contexts.

	Arguments:
	  sock		- An open socket object.  This will be conditioned
	  		  as nonblocking in this object, but in a client context,
			  the caller should already have performed setblocking(0)
			  on it to ensure the system does not block while
			  connecting.
	  parent	- In a server context, this is a "Listener" instance
			  and the channel was instantiated after
			  performing an accept() on a listen socket.
			  If None, this is a client instance and the
			  the socket has been created and a connect()
			  issued.

	Parameters:
	  poll_set	- A poll.poll instance containing this object.
	  		  This is used to control the polling mask based
			  on the presence of output (required).
	  pump		- The Pump-style class to use to deliver I/O.
	  		  (optional, defaults to channel.Pump).
	  log		- A "logging" instance (optional).
"""

	def __init__(self, sock, parent, **params):
		self.parent = parent
		self.params = params
		self.log = getparam(self, 'log', _log_discard)
		self.connection_time = time.time()

		self._pset = getparam(self, 'poll_set')
		if self._pset is None:
			raise Error("poll_set parameter is required")
		if not isinstance(self._pset, poll.poll):
			raise Error("poll_set parameter must be an instance of poll.poll")

		pump_class = getparam(self, 'pump', Pump)
		if not issubclass(pump_class, Pump):
			raise Error("'pump' parameter must be a subclass of Pump")
		self.pump = pump_class(sock, self)
		self._pset.register(self, poll.POLLIN)
		self.closed = False

	def fileno(self):
		"""
		The fileno method is needed so the Channel instance
		can be used in a poll set.
	"""
		return self.pump.fileno()

	def close(self):
		self.pump.close()
		self.closed = True

	def handle(self, chan, mask):
		"""
		Handle poll() event.  This will read available data and/or
		write pending data.  When all pending data has been written,
		poll.POLLOUT is removed for the channel.

		Returns False if the connection is lost, otherwise True.
	"""
		self.log.debug("Handle called")
		if chan != self:
			raise Error("Trying to handle somebody else's channel")
		if mask & poll.POLLOUT:
			if not self.pump.write():
				self._pset.modify(self, poll.POLLIN)
		if mask & poll.POLLIN:
			self.pump.read()
			if self.pump.connection_lost:
				self._pset.unregister(self)
				self.pump.close()
				return False
		return True

	def queue(self, data, **params):
		"""
		This handles outbound encapsulation and is expected to be overridden
		in a subclass.	This base-class operation is to simply queue the data
		as passed and enable poll.POLLOUT on the channel.

		For example, a netstring implemention would add the prefix length
		string and terminator.

		An http implementation would add an HTTP header with content length.
		Protocol-specific params provide a way for the caller to affect the
		header, either in this call, this instance, or for a server context,
		the Listener instance.
	"""
		self.pump.transmitting += data
		self._pset.modify(self, poll.POLLIN|poll.POLLOUT)

	def read(self):
		"""
		This handles inbound decapsulation and is expected to be overridden
		in a subclass.	The base-class operation is to simply return any
		pending data.

		This is implemented as a generator which will continue returning results
		until no further complete messages are available.  Any portion of an
		incomplete message is retained until further I/O completes the message.

		For example, a netstring implemention would check if there is sufficient
		data to determine the message length, and then if the message is
		complete, yielding just the data portion and discarding the complete
		message while retaining any data following the message.

		An http implementation would check that headers have been completely
		read (multiple lines terminated by a double newline), then extract
		the Content-Length (an exception should be raise with if missing),
		then collect data until the message is complete, yielding that data.
		Header information might be exposed to the caller in protocol-specific
		attributes of the subclass.
	"""
		if self.pump.receiving == '':
			return
		data = self.pump.receiving
		self.pump.receiving = ''
		yield data
