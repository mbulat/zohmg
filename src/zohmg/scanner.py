# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# lfm.data.hbase
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase_thrift import Hbase
from hbase_thrift.ttypes import ColumnDescriptor
from hbase_thrift.ttypes import AlreadyExists, IOError, IllegalArgument

# the default behaviour is to scan over all rows
# and return all column families.

class HBaseScanner(object):
#
# Public interface
#

    def __init__(self, h="localhost", p=9090):
        self.host  = h
        self.port  = p
        # these are private
        self.__transport = None
        self.__client    = None
        self.__scannerid = None
        self.__next_row  = None


    def connect(self):
        self.__transport = TSocket.TSocket(self.host, self.port)
        self.__transport = TTransport.TBufferedTransport(self.__transport)
        protocol = TBinaryProtocol.TBinaryProtocol(self.__transport)
        self.__client = Hbase.Client(protocol)
        self.__transport.open()


    def disconnect(self):
        self.__transport.close()


    # opens a scanner and pre-fetches the first row.
    # arguments:
    #   table: the only mandatory argument.
    #   columns: list of column families with optional qualifier.
    #            also accepts a string argument defining a single cfq.
    #   startrow: string defining where to start the scan. will scan all rows if omitted.
    #   stoprow: row key to stop at. the row matching this will be excluded from scan.
    #            will scan to end of table if omitted.
    def open(self, table, columns=[], startrow="", stoprow=""):
        # if columns argument is a string, make into one-element list.
        if columns.__class__ == str:
            columns = [columns]

        # TODO: why does this not work?
        # match all CF's with a regexp, like so:
        #if columns == [] or columns == "":
        #    columns = ["/*/:"] # match all column families.

        try:
            if stoprow == "":
                self.__scannerid = self.__client.scannerOpen(table, startrow, columns)
            else:
                self.__scannerid = self.__client.scannerOpenWithStop(table, startrow, stoprow, columns)
        except IOError:
            # TODO: describe what went wrong, if possible.
            raise ScannerError

        # pre-fetch the first row.
        self.__fetch_row()


    def scanner_ready(self):
        return self.__scannerid != None


    # next/has_next iterator style interface.
    def has_next(self):
        return self.__next_row != None


    def next(self):
        if (not self.scanner_ready()) or (not self.has_next()):
            raise StopIteration
        r = self.__next_row
        self.__fetch_row()
        return r


    # TODO: Make this work.
    # Exception based generator style interface.
    def scan(self):
        try:
            while 1:
                r = self.__next_row
                self.__fetch_row()
                yield r
        except StopIteration:
            raise # No more rows.



#
# Private interface
#

    # only used internally since HBaseScanner raises StopIteration when done.
    def __fetch_row(self):
        try:
            # TODO: scannerGetList() for caching.
            self.__next_row = self.__client.scannerGet(self.__scannerid)
        except IOError:
            raise RowFetchError
        # TODO:
        # the new API returns a list; the empty list implies the old NotFound exception.
        except NotFound:
            # scanner finished, presumably.
            self.__scannerid  = None
            self.__next_row = None
            # TODO: Leaving this out for now.
            #raise StopIteration


#
# Exceptions
#
class RowFetchError(Exception):
    def __init__(self,value):
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)

class ScannerError(Exception):
    pass
