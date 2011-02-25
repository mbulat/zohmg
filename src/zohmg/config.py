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

from zohmg.utils import fail # heh.
import os, re, sys, time


# the configuration file has four parts:
#   'dataset' - string
#   'dimensions' - list of strings
#   'units' - list of strings
#   'projections' - list of lists
#   'aggregations' - list of lists



class ConfigNotLoaded(Exception):
    def __init__(self, value):
        self.error = value
    def __str__(self):
        return self.error



# TODO: multiple dataset files
class Config(object):
    def __init__(self, config_file=None):
        if config_file:
            self.config_file = config_file
        else:
            self.config_file = "dataset.yaml"

        self.config = {}
        self.__read_config()


    def __read_config(self):
        import yaml

        config_loaded    = False
        possible_configs = [self.config_file, "config/"+self.config_file]
        file_loaded = None

        # two error conditions can occur:
        #  A) all files missing/can't be opened. => try next file, report later.
        #  B) a file opens, but can't be parsed. => report imm.

        while (not config_loaded and len(possible_configs) > 0):
            config_file = possible_configs.pop()

            try:
                f = open(config_file, "r")
            except IOError, e:
                continue # try to open the next file.

            # we managed to open the file; will not try and open any more.
            # now, load yaml.
            try:
                self.config = yaml.load(f)
                file_loaded = config_file
            except yaml.scanner.ScannerError, e:
                # condition B.
                # report error immediately.
                sys.stderr.write("Configuration error: could not parse %s.\n" % config_file)
                sys.stderr.write("%s\n", e)
                f.close()
                sys.exit(1)

            # ok, good!
            f.close()
            config_loaded = True


        if not config_loaded:
            # condition A.
            sys.stderr.write("Configuration error: Could not read dataset configuration " \
                              "from any of these files:\n" \
                              "\n".join(possible_configs) + "\n")
            raise ConfigNotLoaded("Could not read configuration file.")

        # check contents.
        if not self.sanity_check():
            msg = "[%s] Configuration error: Could not parse configuration from %s." % (time.asctime(), file_loaded)
            fail(msg) # TODO: should maybe not use fail as it raises SystemExit.

        return self.config


    def dataset(self):
        return self.config['dataset']
    def dimensions(self):
        return self.config['dimensions']
    def units(self):
        return self.config['units']
    def projections(self):
        # turn list of strings into list of list of strings.
        # ['country', 'country-domain-useragent-usertype']
        # => [['country'], ['country', 'domain', 'useragent', 'usertype']]
        return map(lambda s : s.split('-'), self.config['projections'])
    def aggregations(self):
        return self.config['aggregations']

    # returns True if configuration is sane,
    # False otherwise.
    def sanity_check(self):
        sane = True # .. so far.
        try:
            # must be able to read these.
            dataset = self.dataset()
            ds = self.dimensions()
            us = self.units()
            ps = self.projections()
            ag = self.aggregations()
        except:
            # might as well return straight away; nothing else will work.
            print >>sys.stderr, "Configuration error: Missing definition of dataset, dimensions, units, projections."
            return False

        # dimensions, projections and units must be non-empty.
        if ds == None or us == None or ps == None or \
            len(ds) == 0 or len(us) == 0 or len(ps) == 0:
                print >>sys.stderr, "Configuration error: dimensions, projections and units must be non-empty."
                return False

        # also, the configuration may not reference unknown dimensions.
        for p in ps:
            for d in p:
                if d not in ds:
                    print >>sys.stderr, "Configuration error: %s is a reference to an unkown dimension." % d
                    sane = False

        # also, the configuration may not reference unknown units.
        for u in ag.keys():
            if u not in us:
                print >>sys.stderr, "Configuration error: %s is a reference to an unkown unit." % u
                sane = False

        # also, there must be no funny characters in the name of dimensions or units.
        # dimensions and units will feature in the columnfamily name and rowkey respectively.
        for (type, data) in [('dimension', ds), ('unit', us)]:
            for d in data:
                if re.match('^[a-zA-Z0-9]+$', d) == None:
                    print >>sys.stderr, "Configuration error: '%s' is an invalid %s name." % (d, type)
                    sane = False

        # we can be a litte less strict with dataset names since they become the table names.
        if re.match('^[a-zA-Z0-9]+[a-zA-Z0-9-_]*[a-zA-Z0-9]+$', dataset) == None:
            print >>sys.stderr, "Configuration error: '%s' is an invalid dataset name." % dataset
            sane = False

        return sane


class Environ(object):
    def __init__(self):
        self.environ = {}
        self.read_environ()

    def get(self, key):
        try:
            return self.environ[key]
        except:
            return ''

    def read_environ(self):
        # add config path so we can import from it.
        sys.path.append(".")
        sys.path.append("config")

        try:
            import environment
        except ImportError:
            msg = "[%s] Error: Could not import environment.py" % time.asctime()
            fail(msg)

        for key in dir(environment):
            self.environ[key] = environment.__dict__[key]
