#
# Autogenerated by Thrift Compiler (0.9.3)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None


class Iface:
  def login(self, address, conn_id, params):
    """
    Parameters:
     - address
     - conn_id
     - params
    """
    pass

  def ping(self, address, conn_id, context):
    """
    Parameters:
     - address
     - conn_id
     - context
    """
    pass

  def disconnect(self, address, conn_id, context):
    """
    Parameters:
     - address
     - conn_id
     - context
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def login(self, address, conn_id, params):
    """
    Parameters:
     - address
     - conn_id
     - params
    """
    self.send_login(address, conn_id, params)

  def send_login(self, address, conn_id, params):
    self._oprot.writeMessageBegin('login', TMessageType.ONEWAY, self._seqid)
    args = login_args()
    args.address = address
    args.conn_id = conn_id
    args.params = params
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()
  def ping(self, address, conn_id, context):
    """
    Parameters:
     - address
     - conn_id
     - context
    """
    self.send_ping(address, conn_id, context)

  def send_ping(self, address, conn_id, context):
    self._oprot.writeMessageBegin('ping', TMessageType.ONEWAY, self._seqid)
    args = ping_args()
    args.address = address
    args.conn_id = conn_id
    args.context = context
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()
  def disconnect(self, address, conn_id, context):
    """
    Parameters:
     - address
     - conn_id
     - context
    """
    self.send_disconnect(address, conn_id, context)

  def send_disconnect(self, address, conn_id, context):
    self._oprot.writeMessageBegin('disconnect', TMessageType.ONEWAY, self._seqid)
    args = disconnect_args()
    args.address = address
    args.conn_id = conn_id
    args.context = context
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["login"] = Processor.process_login
    self._processMap["ping"] = Processor.process_ping
    self._processMap["disconnect"] = Processor.process_disconnect

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return
    else:
      self._processMap[name](self, seqid, iprot, oprot)
    return True

  def process_login(self, seqid, iprot, oprot):
    args = login_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.login(args.address, args.conn_id, args.params)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass

  def process_ping(self, seqid, iprot, oprot):
    args = ping_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.ping(args.address, args.conn_id, args.context)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass

  def process_disconnect(self, seqid, iprot, oprot):
    args = disconnect_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.disconnect(args.address, args.conn_id, args.context)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass


# HELPER FUNCTIONS AND STRUCTURES

class login_args:
  """
  Attributes:
   - address
   - conn_id
   - params
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'address', None, None, ), # 1
    (2, TType.STRING, 'conn_id', None, None, ), # 2
    (3, TType.MAP, 'params', (TType.STRING,None,TType.STRING,None), None, ), # 3
  )

  def __init__(self, address=None, conn_id=None, params=None,):
    self.address = address
    self.conn_id = conn_id
    self.params = params

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.address = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.conn_id = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.MAP:
          self.params = {}
          (_ktype8, _vtype9, _size7 ) = iprot.readMapBegin()
          for _i11 in xrange(_size7):
            _key12 = iprot.readString()
            _val13 = iprot.readString()
            self.params[_key12] = _val13
          iprot.readMapEnd()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('login_args')
    if self.address is not None:
      oprot.writeFieldBegin('address', TType.STRING, 1)
      oprot.writeString(self.address)
      oprot.writeFieldEnd()
    if self.conn_id is not None:
      oprot.writeFieldBegin('conn_id', TType.STRING, 2)
      oprot.writeString(self.conn_id)
      oprot.writeFieldEnd()
    if self.params is not None:
      oprot.writeFieldBegin('params', TType.MAP, 3)
      oprot.writeMapBegin(TType.STRING, TType.STRING, len(self.params))
      for kiter14,viter15 in self.params.items():
        oprot.writeString(kiter14)
        oprot.writeString(viter15)
      oprot.writeMapEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.address)
    value = (value * 31) ^ hash(self.conn_id)
    value = (value * 31) ^ hash(self.params)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class ping_args:
  """
  Attributes:
   - address
   - conn_id
   - context
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'address', None, None, ), # 1
    (2, TType.STRING, 'conn_id', None, None, ), # 2
    (3, TType.STRING, 'context', None, None, ), # 3
  )

  def __init__(self, address=None, conn_id=None, context=None,):
    self.address = address
    self.conn_id = conn_id
    self.context = context

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.address = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.conn_id = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.context = iprot.readString()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('ping_args')
    if self.address is not None:
      oprot.writeFieldBegin('address', TType.STRING, 1)
      oprot.writeString(self.address)
      oprot.writeFieldEnd()
    if self.conn_id is not None:
      oprot.writeFieldBegin('conn_id', TType.STRING, 2)
      oprot.writeString(self.conn_id)
      oprot.writeFieldEnd()
    if self.context is not None:
      oprot.writeFieldBegin('context', TType.STRING, 3)
      oprot.writeString(self.context)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.address)
    value = (value * 31) ^ hash(self.conn_id)
    value = (value * 31) ^ hash(self.context)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class disconnect_args:
  """
  Attributes:
   - address
   - conn_id
   - context
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'address', None, None, ), # 1
    (2, TType.STRING, 'conn_id', None, None, ), # 2
    (3, TType.STRING, 'context', None, None, ), # 3
  )

  def __init__(self, address=None, conn_id=None, context=None,):
    self.address = address
    self.conn_id = conn_id
    self.context = context

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.address = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.conn_id = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.context = iprot.readString()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('disconnect_args')
    if self.address is not None:
      oprot.writeFieldBegin('address', TType.STRING, 1)
      oprot.writeString(self.address)
      oprot.writeFieldEnd()
    if self.conn_id is not None:
      oprot.writeFieldBegin('conn_id', TType.STRING, 2)
      oprot.writeString(self.conn_id)
      oprot.writeFieldEnd()
    if self.context is not None:
      oprot.writeFieldBegin('context', TType.STRING, 3)
      oprot.writeString(self.context)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.address)
    value = (value * 31) ^ hash(self.conn_id)
    value = (value * 31) ^ hash(self.context)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)
