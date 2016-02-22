#!/usr/bin/python


# Quick and dirty (very!) check for CQ and RP on influxdb

from influxdb import InfluxDBClient
import collections
import dateutil.parser
from datetime import datetime, timedelta
import pytz
from termcolor import colored

class InfluxCheck:
    '''
    Class to check influxdb data
    '''


    def __init__(self, server='localhost', port=8086,login='', password='', db='db', ssl=False):
        self.influxdb_server = server
        self.influxdb_port = port
        self.influxdb_login = login
        self.influxdb_password = password
        self.influxdb_db = db
        self.cqs = collections.OrderedDict()
        self.servers = []
        self.measurements = []
        try:
            self.client = InfluxDBClient(self.influxdb_server, 8086, self.influxdb_login, self.influxdb_password, self.influxdb_db, ssl)
        except Exception as e:
            print "Could not connect to influxdb: %s" % (e)
            exit -1


    def add_cq(self, name, datapoints, interval):
        '''
        Add a CQ with the expected number of datapoints
        
        name:
                name of the CQ must match the name of the RP ( retention policy) too 
        datapoints:
                number of expected datapoints ( retention policy duration * group by time in CQ , in minutes)
        interval:
                how often the CQ is running ( in minutes)
        '''
        if name and datapoints:
            self.cqs[name] = {'dp': datapoints, 'interval': interval}


    def add_servers(self, filename='servers'):
        '''
        Add servers from filename, must be separated by newline
        '''
        try:
            f = open('servers', 'r')
            for server in f.read().splitlines():
                self.servers.append(server)
            f.close()
        except Exception as e:
            print "Could not open host file: %s" % (e)
            exit(-1)

    def add_server(self, server):
        '''
        Add one server to the list of servers to check
        '''
        if server:
            self.servers.append(server)
    
    def add_measurement(self, measurement):
        '''
        Add measurement to check
        '''
        if measurement:
            self.measurements.append(measurement)

    def find_time_1st_entry(self, cq, measurement, server):
        '''
        Find the first entry that has been insert in the RP ( or the latest one if we 
        are flushing entries regularly. In the case of a long CQ it is unlikely we will 
        get all the datapoints as there is not backfilling.
        '''
        query="select value from \"%s\".\"%s\" where host = '%s' order by time asc limit 1" % (cq, measurement, server)
        try:
            resultset = self.client.query(query)
            for res in resultset:
                return res[0]['time']
        except Exception as e:
            return -1

    def test_cqs(self):
        '''
        Test the CQs and output the results:
                red is CRITICAL
                yellow is WARNING => it is not necessarly a problem, could just be that the CQ is over a year for eg
                green is OK
        '''
        print "Server\t\tMeasurement\t\tRP\t\tDatapoints\t\tExpected\t\tExpected from first insert\n========================================================================"
        if self.cqs and self.servers and self.measurements:
            for cq in self.cqs:
                for measurement in self.measurements:
                    for server in self.servers:
                        query="select count(value) from \"%s\".\"%s\" where host = '%s' order by time desc" % (cq, measurement, server)
                        try:
                            resultset = self.client.query(query)
                            firstentry = self.find_time_1st_entry(cq, measurement, server)
                            color = 'red'
                            expectFrom1st=0
                            count = -1
                            for res in resultset:
                                count = res[0]['count']
                                break
                            if firstentry:
                                date = dateutil.parser.parse(firstentry)
                                diff = datetime.now(pytz.utc) - date
                                _1st = (diff.total_seconds() / 60)
                                expectFrom1st =  (int) (_1st / self.cqs[cq]['interval']) 
                                color = 'green'
                                if (count  < expectFrom1st and count < self.cqs[cq]['dp'] and expectFrom1st) or not expectFrom1st:
                                    color = 'red'
                                elif count < self.cqs[cq]['dp'] and count >= expectFrom1st and expectFrom1st:
                                    color = 'yellow'

                            print colored("%s\t%s\t%s\t\t%s\t\t\t%s\t\t\t%s" % (server, measurement, cq, count, self.cqs[cq]['dp'], expectFrom1st), color)
                        except Exception as e:
                            print "Error query measurement %s for %s on retention policy %s (%s)" % (measurement, server, cq, e)
                print "\n"


test = InfluxCheck(db='db')
test.add_servers() # add servers from the servers file
test.add_server('server4') # add a server manually
test.add_measurement('load-load-midterm') # add a measurement to check

test.add_cq('_1h', 60, 1) # add CQ to check 
test.add_cq('_1d', 288, 5)
test.add_cq('_1w', 336, 30)
test.add_cq('_4w', 336, 120)
test.add_cq('_26w', 364, 720)
test.add_cq('_52w', 364, 1440)


test.test_cqs() # test and output results
