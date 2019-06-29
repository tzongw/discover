#
# Autogenerated by Thrift Compiler (0.12.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import logging
from .ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
all_structs = []


class Iface(object):
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

    def send_text(self, conn_id, message):
        """
        Parameters:
         - conn_id
         - message

        """
        pass

    def send_binary(self, conn_id, message):
        """
        Parameters:
         - conn_id
         - message

        """
        pass

    def join_group(self, conn_id, group):
        """
        Parameters:
         - conn_id
         - group

        """
        pass

    def leave_group(self, conn_id, group):
        """
        Parameters:
         - conn_id
         - group

        """
        pass

    def broadcast_binary(self, group, exclude, message):
        """
        Parameters:
         - group
         - exclude
         - message

        """
        pass

    def broadcast_text(self, group, exclude, message):
        """
        Parameters:
         - group
         - exclude
         - message

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

    def send_text(self, conn_id, message):
        """
        Parameters:
         - conn_id
         - message

        """
        self.send_send_text(conn_id, message)

    def send_send_text(self, conn_id, message):
        self._oprot.writeMessageBegin('send_text', TMessageType.ONEWAY, self._seqid)
        args = send_text_args()
        args.conn_id = conn_id
        args.message = message
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def send_binary(self, conn_id, message):
        """
        Parameters:
         - conn_id
         - message

        """
        self.send_send_binary(conn_id, message)

    def send_send_binary(self, conn_id, message):
        self._oprot.writeMessageBegin('send_binary', TMessageType.ONEWAY, self._seqid)
        args = send_binary_args()
        args.conn_id = conn_id
        args.message = message
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def join_group(self, conn_id, group):
        """
        Parameters:
         - conn_id
         - group

        """
        self.send_join_group(conn_id, group)

    def send_join_group(self, conn_id, group):
        self._oprot.writeMessageBegin('join_group', TMessageType.ONEWAY, self._seqid)
        args = join_group_args()
        args.conn_id = conn_id
        args.group = group
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def leave_group(self, conn_id, group):
        """
        Parameters:
         - conn_id
         - group

        """
        self.send_leave_group(conn_id, group)

    def send_leave_group(self, conn_id, group):
        self._oprot.writeMessageBegin('leave_group', TMessageType.ONEWAY, self._seqid)
        args = leave_group_args()
        args.conn_id = conn_id
        args.group = group
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def broadcast_binary(self, group, exclude, message):
        """
        Parameters:
         - group
         - exclude
         - message

        """
        self.send_broadcast_binary(group, exclude, message)

    def send_broadcast_binary(self, group, exclude, message):
        self._oprot.writeMessageBegin('broadcast_binary', TMessageType.ONEWAY, self._seqid)
        args = broadcast_binary_args()
        args.group = group
        args.exclude = exclude
        args.message = message
        args.write(self._oprot)
        self._oprot.writeMessageEnd()
        self._oprot.trans.flush()

    def broadcast_text(self, group, exclude, message):
        """
        Parameters:
         - group
         - exclude
         - message

        """
        self.send_broadcast_text(group, exclude, message)

    def send_broadcast_text(self, group, exclude, message):
        self._oprot.writeMessageBegin('broadcast_text', TMessageType.ONEWAY, self._seqid)
        args = broadcast_text_args()
        args.group = group
        args.exclude = exclude
        args.message = message
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
        self._processMap["send_text"] = Processor.process_send_text
        self._processMap["send_binary"] = Processor.process_send_binary
        self._processMap["join_group"] = Processor.process_join_group
        self._processMap["leave_group"] = Processor.process_leave_group
        self._processMap["broadcast_binary"] = Processor.process_broadcast_binary
        self._processMap["broadcast_text"] = Processor.process_broadcast_text

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
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_unset_context(self, seqid, iprot, oprot):
        args = unset_context_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.unset_context(args.conn_id, args.context)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_remove_conn(self, seqid, iprot, oprot):
        args = remove_conn_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.remove_conn(args.conn_id)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_send_text(self, seqid, iprot, oprot):
        args = send_text_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.send_text(args.conn_id, args.message)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_send_binary(self, seqid, iprot, oprot):
        args = send_binary_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.send_binary(args.conn_id, args.message)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_join_group(self, seqid, iprot, oprot):
        args = join_group_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.join_group(args.conn_id, args.group)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_leave_group(self, seqid, iprot, oprot):
        args = leave_group_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.leave_group(args.conn_id, args.group)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_broadcast_binary(self, seqid, iprot, oprot):
        args = broadcast_binary_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.broadcast_binary(args.group, args.exclude, args.message)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

    def process_broadcast_text(self, seqid, iprot, oprot):
        args = broadcast_text_args()
        args.read(iprot)
        iprot.readMessageEnd()
        try:
            self._handler.broadcast_text(args.group, args.exclude, args.message)
        except TTransport.TTransportException:
            raise
        except Exception:
            logging.exception('Exception in oneway handler')

# HELPER FUNCTIONS AND STRUCTURES


class set_context_args(object):
    """
    Attributes:
     - conn_id
     - context

    """


    def __init__(self, conn_id=None, context=None,):
        self.conn_id = conn_id
        self.context = context

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.conn_id = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.context = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('set_context_args')
        if self.conn_id is not None:
            oprot.writeFieldBegin('conn_id', TType.STRING, 1)
            oprot.writeString(self.conn_id.encode('utf-8') if sys.version_info[0] == 2 else self.conn_id)
            oprot.writeFieldEnd()
        if self.context is not None:
            oprot.writeFieldBegin('context', TType.STRING, 2)
            oprot.writeString(self.context.encode('utf-8') if sys.version_info[0] == 2 else self.context)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(set_context_args)
set_context_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'conn_id', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'context', 'UTF8', None, ),  # 2
)


class unset_context_args(object):
    """
    Attributes:
     - conn_id
     - context

    """


    def __init__(self, conn_id=None, context=None,):
        self.conn_id = conn_id
        self.context = context

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.conn_id = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.SET:
                    self.context = set()
                    (_etype3, _size0) = iprot.readSetBegin()
                    for _i4 in range(_size0):
                        _elem5 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.context.add(_elem5)
                    iprot.readSetEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('unset_context_args')
        if self.conn_id is not None:
            oprot.writeFieldBegin('conn_id', TType.STRING, 1)
            oprot.writeString(self.conn_id.encode('utf-8') if sys.version_info[0] == 2 else self.conn_id)
            oprot.writeFieldEnd()
        if self.context is not None:
            oprot.writeFieldBegin('context', TType.SET, 2)
            oprot.writeSetBegin(TType.STRING, len(self.context))
            for iter6 in self.context:
                oprot.writeString(iter6.encode('utf-8') if sys.version_info[0] == 2 else iter6)
            oprot.writeSetEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(unset_context_args)
unset_context_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'conn_id', 'UTF8', None, ),  # 1
    (2, TType.SET, 'context', (TType.STRING, 'UTF8', False), None, ),  # 2
)


class remove_conn_args(object):
    """
    Attributes:
     - conn_id

    """


    def __init__(self, conn_id=None,):
        self.conn_id = conn_id

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.conn_id = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('remove_conn_args')
        if self.conn_id is not None:
            oprot.writeFieldBegin('conn_id', TType.STRING, 1)
            oprot.writeString(self.conn_id.encode('utf-8') if sys.version_info[0] == 2 else self.conn_id)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(remove_conn_args)
remove_conn_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'conn_id', 'UTF8', None, ),  # 1
)


class send_text_args(object):
    """
    Attributes:
     - conn_id
     - message

    """


    def __init__(self, conn_id=None, message=None,):
        self.conn_id = conn_id
        self.message = message

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.conn_id = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.message = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('send_text_args')
        if self.conn_id is not None:
            oprot.writeFieldBegin('conn_id', TType.STRING, 1)
            oprot.writeString(self.conn_id.encode('utf-8') if sys.version_info[0] == 2 else self.conn_id)
            oprot.writeFieldEnd()
        if self.message is not None:
            oprot.writeFieldBegin('message', TType.STRING, 2)
            oprot.writeString(self.message.encode('utf-8') if sys.version_info[0] == 2 else self.message)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(send_text_args)
send_text_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'conn_id', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'message', 'UTF8', None, ),  # 2
)


class send_binary_args(object):
    """
    Attributes:
     - conn_id
     - message

    """


    def __init__(self, conn_id=None, message=None,):
        self.conn_id = conn_id
        self.message = message

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.conn_id = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.message = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('send_binary_args')
        if self.conn_id is not None:
            oprot.writeFieldBegin('conn_id', TType.STRING, 1)
            oprot.writeString(self.conn_id.encode('utf-8') if sys.version_info[0] == 2 else self.conn_id)
            oprot.writeFieldEnd()
        if self.message is not None:
            oprot.writeFieldBegin('message', TType.STRING, 2)
            oprot.writeBinary(self.message)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(send_binary_args)
send_binary_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'conn_id', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'message', 'BINARY', None, ),  # 2
)


class join_group_args(object):
    """
    Attributes:
     - conn_id
     - group

    """


    def __init__(self, conn_id=None, group=None,):
        self.conn_id = conn_id
        self.group = group

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.conn_id = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.group = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('join_group_args')
        if self.conn_id is not None:
            oprot.writeFieldBegin('conn_id', TType.STRING, 1)
            oprot.writeString(self.conn_id.encode('utf-8') if sys.version_info[0] == 2 else self.conn_id)
            oprot.writeFieldEnd()
        if self.group is not None:
            oprot.writeFieldBegin('group', TType.STRING, 2)
            oprot.writeString(self.group.encode('utf-8') if sys.version_info[0] == 2 else self.group)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(join_group_args)
join_group_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'conn_id', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'group', 'UTF8', None, ),  # 2
)


class leave_group_args(object):
    """
    Attributes:
     - conn_id
     - group

    """


    def __init__(self, conn_id=None, group=None,):
        self.conn_id = conn_id
        self.group = group

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.conn_id = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.group = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('leave_group_args')
        if self.conn_id is not None:
            oprot.writeFieldBegin('conn_id', TType.STRING, 1)
            oprot.writeString(self.conn_id.encode('utf-8') if sys.version_info[0] == 2 else self.conn_id)
            oprot.writeFieldEnd()
        if self.group is not None:
            oprot.writeFieldBegin('group', TType.STRING, 2)
            oprot.writeString(self.group.encode('utf-8') if sys.version_info[0] == 2 else self.group)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(leave_group_args)
leave_group_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'conn_id', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'group', 'UTF8', None, ),  # 2
)


class broadcast_binary_args(object):
    """
    Attributes:
     - group
     - exclude
     - message

    """


    def __init__(self, group=None, exclude=None, message=None,):
        self.group = group
        self.exclude = exclude
        self.message = message

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.group = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.SET:
                    self.exclude = set()
                    (_etype10, _size7) = iprot.readSetBegin()
                    for _i11 in range(_size7):
                        _elem12 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.exclude.add(_elem12)
                    iprot.readSetEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.message = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('broadcast_binary_args')
        if self.group is not None:
            oprot.writeFieldBegin('group', TType.STRING, 1)
            oprot.writeString(self.group.encode('utf-8') if sys.version_info[0] == 2 else self.group)
            oprot.writeFieldEnd()
        if self.exclude is not None:
            oprot.writeFieldBegin('exclude', TType.SET, 2)
            oprot.writeSetBegin(TType.STRING, len(self.exclude))
            for iter13 in self.exclude:
                oprot.writeString(iter13.encode('utf-8') if sys.version_info[0] == 2 else iter13)
            oprot.writeSetEnd()
            oprot.writeFieldEnd()
        if self.message is not None:
            oprot.writeFieldBegin('message', TType.STRING, 3)
            oprot.writeBinary(self.message)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(broadcast_binary_args)
broadcast_binary_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'group', 'UTF8', None, ),  # 1
    (2, TType.SET, 'exclude', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.STRING, 'message', 'BINARY', None, ),  # 3
)


class broadcast_text_args(object):
    """
    Attributes:
     - group
     - exclude
     - message

    """


    def __init__(self, group=None, exclude=None, message=None,):
        self.group = group
        self.exclude = exclude
        self.message = message

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.group = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.SET:
                    self.exclude = set()
                    (_etype17, _size14) = iprot.readSetBegin()
                    for _i18 in range(_size14):
                        _elem19 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.exclude.add(_elem19)
                    iprot.readSetEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.message = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('broadcast_text_args')
        if self.group is not None:
            oprot.writeFieldBegin('group', TType.STRING, 1)
            oprot.writeString(self.group.encode('utf-8') if sys.version_info[0] == 2 else self.group)
            oprot.writeFieldEnd()
        if self.exclude is not None:
            oprot.writeFieldBegin('exclude', TType.SET, 2)
            oprot.writeSetBegin(TType.STRING, len(self.exclude))
            for iter20 in self.exclude:
                oprot.writeString(iter20.encode('utf-8') if sys.version_info[0] == 2 else iter20)
            oprot.writeSetEnd()
            oprot.writeFieldEnd()
        if self.message is not None:
            oprot.writeFieldBegin('message', TType.STRING, 3)
            oprot.writeString(self.message.encode('utf-8') if sys.version_info[0] == 2 else self.message)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(broadcast_text_args)
broadcast_text_args.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'group', 'UTF8', None, ),  # 1
    (2, TType.SET, 'exclude', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.STRING, 'message', 'UTF8', None, ),  # 3
)
fix_spec(all_structs)
del all_structs

