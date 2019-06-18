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
  def set_context(self, conn_id, context):
    """
    Parameters:
     - conn_id
     - context
    """
    pass

  def unset_context(self, conn_id, context):
    """
    Parameters:
     - conn_id
     - context
    """
    pass

  def remove_conn(self, conn_id):
    """
    Parameters:
     - conn_id
    """
    pass


class Client(Iface):
  def __init__(self, iprot, oprot=None):
    self._iprot = self._oprot = iprot
    if oprot is not None:
      self._oprot = oprot
    self._seqid = 0

  def set_context(self, conn_id, context):
    """
    Parameters:
     - conn_id
     - context
    """
    self.send_set_context(conn_id, context)

  def send_set_context(self, conn_id, context):
    self._oprot.writeMessageBegin('set_context', TMessageType.ONEWAY, self._seqid)
    args = set_context_args()
    args.conn_id = conn_id
    args.context = context
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()
  def unset_context(self, conn_id, context):
    """
    Parameters:
     - conn_id
     - context
    """
    self.send_unset_context(conn_id, context)

  def send_unset_context(self, conn_id, context):
    self._oprot.writeMessageBegin('unset_context', TMessageType.ONEWAY, self._seqid)
    args = unset_context_args()
    args.conn_id = conn_id
    args.context = context
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()
  def remove_conn(self, conn_id):
    """
    Parameters:
     - conn_id
    """
    self.send_remove_conn(conn_id)

  def send_remove_conn(self, conn_id):
    self._oprot.writeMessageBegin('remove_conn', TMessageType.ONEWAY, self._seqid)
    args = remove_conn_args()
    args.conn_id = conn_id
    args.write(self._oprot)
    self._oprot.writeMessageEnd()
    self._oprot.trans.flush()

class Processor(Iface, TProcessor):
  def __init__(self, handler):
    self._handler = handler
    self._processMap = {}
    self._processMap["set_context"] = Processor.process_set_context
    self._processMap["unset_context"] = Processor.process_unset_context
    self._processMap["remove_conn"] = Processor.process_remove_conn

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

  def process_set_context(self, seqid, iprot, oprot):
    args = set_context_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.set_context(args.conn_id, args.context)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass

  def process_unset_context(self, seqid, iprot, oprot):
    args = unset_context_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.unset_context(args.conn_id, args.context)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass

  def process_remove_conn(self, seqid, iprot, oprot):
    args = remove_conn_args()
    args.read(iprot)
    iprot.readMessageEnd()
    try:
      self._handler.remove_conn(args.conn_id)
      msg_type = TMessageType.REPLY
    except (TTransport.TTransportException, KeyboardInterrupt, SystemExit):
      raise
    except:
      pass


# HELPER FUNCTIONS AND STRUCTURES

class set_context_args:
  """
  Attributes:
   - conn_id
   - context
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'conn_id', None, None, ), # 1
    (2, TType.STRING, 'context', None, None, ), # 2
  )

  def __init__(self, conn_id=None, context=None,):
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
          self.conn_id = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
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
    oprot.writeStructBegin('set_context_args')
    if self.conn_id is not None:
      oprot.writeFieldBegin('conn_id', TType.STRING, 1)
      oprot.writeString(self.conn_id)
      oprot.writeFieldEnd()
    if self.context is not None:
      oprot.writeFieldBegin('context', TType.STRING, 2)
      oprot.writeString(self.context)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
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

class unset_context_args:
  """
  Attributes:
   - conn_id
   - context
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'conn_id', None, None, ), # 1
    (2, TType.SET, 'context', (TType.STRING,None), None, ), # 2
  )

  def __init__(self, conn_id=None, context=None,):
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
          self.conn_id = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.SET:
          self.context = set()
          (_etype3, _size0) = iprot.readSetBegin()
          for _i4 in xrange(_size0):
            _elem5 = iprot.readString()
            self.context.add(_elem5)
          iprot.readSetEnd()
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
    oprot.writeStructBegin('unset_context_args')
    if self.conn_id is not None:
      oprot.writeFieldBegin('conn_id', TType.STRING, 1)
      oprot.writeString(self.conn_id)
      oprot.writeFieldEnd()
    if self.context is not None:
      oprot.writeFieldBegin('context', TType.SET, 2)
      oprot.writeSetBegin(TType.STRING, len(self.context))
      for iter6 in self.context:
        oprot.writeString(iter6)
      oprot.writeSetEnd()
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
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

class remove_conn_args:
  """
  Attributes:
   - conn_id
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'conn_id', None, None, ), # 1
  )

  def __init__(self, conn_id=None,):
    self.conn_id = conn_id

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
          self.conn_id = iprot.readString()
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
    oprot.writeStructBegin('remove_conn_args')
    if self.conn_id is not None:
      oprot.writeFieldBegin('conn_id', TType.STRING, 1)
      oprot.writeString(self.conn_id)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.conn_id)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)