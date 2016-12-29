#!/usr/bin/env python
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

class Error(Exception): pass

class Listener(object):
	"""
	A Listener is defined to handle connections for one or more listen sockets,
	which must all be registered in poll_set, an instance of poll.poll.  When a
	poll indicates input on a listen socket, a new instance of channel_class will
	be created to handle I/O on the incomming connection.  These channel_class
	instances will be added and removed as necessary from the poll instance.

	Parameters:

	  log		- A "logging" instance.
	  closeall	- If True (default), all listen sockets will be closed
	  		  when the context exits.  If False, only channels
			  are closed.
"""
	def __init__(self, poll_set, channel_class, **params):
		self.log = params.get('log')
		self.closeall = params.get('closeall', True)
		if not self.log:
			self.log = logging.getLogger(__name__)
			self.log.addHandler(logging.NullHandler())

		self._pset = poll_set
		self._listeners = set()
		self._channels = set()
		if not issubclass(channel_class, Channel):
			raise Error("channel_class argument must be a subclass of Channel")
		self._channel_class = channel_class

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.deafen()
		self.reset()

	def listen(self, socks):
		if hasattr(socks, '__iter__'):
			for sock in socks:
				self._listeners.add(sock)
		else:
			self._listeners.add(socks)

	def deafen(self, socks=None):
		if socks is None:
			socks = self._listeners
		for sock in socks:
			if self.closeall:
				try: os.close(sock)
				except: pass
			s.discard(sock)

	def handle(self, event_list):
		"""
		Perform operations as needed for all events returned by a poll() call.
		The list may include objects not managed by this channel_set.  These
		are just ignored.  The scope() method can be used to determine if an
		object is in the channel set.
	"""
		for chan, mask in event_list:
			if chan in self._channels:
				chan._process(mask)
			elif chan in self._listeners:
				self.log.info("New connection detected")
				sock, client = chan.accept()
				self.log.debug("New connection from %s", repr(client))
				self._channels.add(self.channel_class(sock, listener=self))
			else:
				self.log.debug("Event out of context for this channel_set")
	
	def scope(self, chan):
		"""
		Return True if this channel set can handle the
		event object.
	"""
		return (chan in self._listeners or chan in self._channels)

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

class Channel(object):
	"""
	Handles channel communication.

	A channel is created to handle I/O on a socket.  It operates
	in both client and server contexts.

	Parameters:

	  log		- A "logging" instance.
	  listener	- A "Listener" instance.  If present, the
	  		  channel is operating in a server context
			  and the channel was instantiated after
			  performing an accept() on a listen socket.
"""

	def __init__(self, sock, **params):
		self._sock = sock
		self.connection_time = time.time()
		self._listener = params.get('listener', None)
		self.log = params.get('log')
		if not self.log and self._listener is not None:
			self.log = self._listener.log
		if not self.log:
			self.log = logging.getLogger(__name__)
			self.log.addHandler(logging.NullHandler())

	def fileno(self):
		"""
		The fileno method is needed so the Channel instance
		can be used in a poll set.
	"""
		return self._sock.fileno()

	def handle(self, mask):
		"""
		Handle poll() event.  This will read available data and/or
		write pending data.  When all pending data has been written,
		the POLLOUT is removed from the pset.

		process() mis a callback, but need a better approach.
	"""
		self.log.debug("Handle called")
		if mask & poll.POLLIN:
			for item in self.pump._read():
				self.process(item)
			if self.reader.connection_lost:
				self.log.info("EOF on %s connection", repr(self.client))
				self.close()

		if mask & poll.POLLOUT:
			self.pump._write()
			if self.pump.queue() == 0:
				self.channel_set._pset.modify(self, poll.POLLIN)
