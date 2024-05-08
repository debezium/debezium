#!/usr/bin/env python3

from kafka import KafkaConsumer
from kafka import KafkaAdminClient
import json
from pprint import pprint
import jaydebeapi
import sys
import time
import random
import string
from pprint import pprint
import requests
import datetime
import threading
import jpype


tpchomedir = '/home/tpc'
table = ''
lowercase = 'false'


def initsql(conn, config, tpcconfig):
    curs = conn.cursor()
    for sql in tpcconfig['jdbc'][config['config']['connector.class'].split('.')[3]]['initsql']:
        if sql.startswith('python.time.sleep'):
            print('Wait for ' + sql[17:] + ' second')
            time.sleep(int(sql[17:]))
        else:
            try:
                curs.execute(sql)
                conn.commit()
                print(sql)
            except:
                print(sql + ' done with exception  !!')
    return 0


def createTPCTable(conn, config, tpcconfig):
    curs = conn.cursor()
    try:
        print(tpcconfig['jdbc'][config['config']
                                ['connector.class'].split('.')[3]]['tpctable'])
        curs.execute(tpcconfig['jdbc'][config['config']
                                       ['connector.class'].split('.')[3]]['tpctable'])
        conn.commit()
        print('table created')
        return 0
    except:
        print('table create error')
        return 1


def enablecdctablesql(conn, config, tpcconfig):
    curs = conn.cursor()
    for sql in tpcconfig['jdbc'][config['config']['connector.class'].split('.')[3]]['enablecdctablesql']:
        if sql.startswith('python.time.sleep'):
            print('Wait for ' + sql[17:] + ' second')
            time.sleep(int(sql[17:]))
        else:
            try:
                curs.execute(sql)
                conn.commit()
                print(sql)
            except:
                print(sql + ' done with exception  !!')
    return 0


def topicexport(bootstrapserver, topicname, count, commitinterval):
    global tpchomedir
    consumer = KafkaConsumer(topicname,
                             bootstrap_servers=bootstrapserver,
                             auto_offset_reset='smallest',
                             enable_auto_commit=True,
                             )
    i = 0
    file = open(tpchomedir + "/tpcdata/" + "tpc_" + str(count) +
                "_" + str(commitinterval) + ".csv", "w")
    print('read from TOPIC')
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

        d = json.loads(message.value)
        idf = 'ID'
        t0f = 'T0'
        if lowercase:
            idf = 'id'
            t0f = 't0'
        file.write(str(message.timestamp) + "000;" + str(d['payload']['after'][t0f]) +
                   ";" + d['payload']['op'] + ";" + str(i) + ";" + str(d['payload']['after'][idf]) + "\n")
        i = i + 1
        print(i)
        print(count)
        if i >= count:
            break
    file.close()


def getjdbcconnection(config, tpcconfig, connectiontype):
    global tpchomedir
    jdbctype = 'jdbc:' + connectiontype + '://'
    if connectiontype == 'oracle':
        jdbctype = 'jdbc:oracle:thin:@'
    conn = jaydebeapi.connect(tpcconfig['jdbc'][connectiontype]['jdbcdriver'], jdbctype +
                              config['config']['database.hostname'] + ':' +
                              config['config']['database.port'] + '/' +
                              config['config']['database.dbname'],
                              [config['config']['database.user'],
                                  config['config']['database.password']],
                              tpchomedir + '/jdbcdriver/' + tpcconfig['jdbc'][connectiontype]['jar'])
    return conn


def main(argv):
    with open('register.json') as f:
        config = json.load(f)
    with open('tpc-config.json') as f:
        tpcconfig = json.load(f)

    if (len(argv) > 0):
        bootstrapserver = argv[0]
    print(config['config']['connector.class'])
    print(config['name'])
    config['name'] = 'tpc-connector'
    print(config['name'])

    config['config']['schema.history.internal.kafka.topic'] = 'tpc-test'

    databasetype = config['config']['connector.class']
    connectiontype = config['config']['connector.class'].split('.')[3]
    print(databasetype)
    print(connectiontype)

    table = tpcconfig['jdbc'][connectiontype].get('table')
    if table == None:
        table = 'TPC.TEST'
    config['config']['table.include.list'] = table

    lowercase = tpcconfig['jdbc'][connectiontype].get('lowercase')
    if lowercase == None:
        lowercase = False

    conn = getjdbcconnection(config, tpcconfig, connectiontype)

    initsql(conn, config, tpcconfig)
    createTPCTable(conn, config, tpcconfig)
    enablecdctablesql(conn, config, tpcconfig)

    print('============')

    print('get status tpc connector')
    resp = requests.get(
        'http://' + tpcconfig['debezium.connect.server'] + '/connectors/tpc-connector/status',  verify=False)
    print(resp.content)
    print(resp.status_code)
    retvalue = json.loads(resp.content)
    print(retvalue)

    print('============')

    resp = requests.delete(
        'http://' + tpcconfig['debezium.connect.server'] + '/connectors/tpc-connector',  verify=False)
    print(resp.content)
    print(resp.status_code)
    if (resp.status_code == 404):
        print('tpc-connector not exists')
    else:
        print('tpc-connector deleted')
        pass

    databaseservername = config['config']['topic.prefix']
    topicname = databaseservername + '.' + table
    historybootstrapserver = config['config'].get('schema.history.internal.kafka.bootstrap.servers')
    if historybootstrapserver != None:
        bootstrapserver = historybootstrapserver.split(",")

    # check integrated test ( all in one docker)
    if bootstrapserver == 'kafka:9092':

        print(bootstrapserver)
        kafkaadmin = KafkaAdminClient(bootstrap_servers=bootstrapserver)

        try:
            kafkaadmin.delete_topics(
                [topicname], 30)
        except:
            print(topicname + ' TOPIC not exists')
        else:
            print(topicname + ' TOPIC deleted')
        if historybootstrapserver != None:
            try:
                kafkaadmin.delete_topics(
                    [config['config']['schema.history.internal.kafka.topic']], 30)
            except:
                print(config['config']['schema.history.internal.kafka.topic'] +
                      ' TOPIC not exists')
            else:
                print(config['config']
                      ['schema.history.internal.kafka.topic'] + ' TOPIC deleted')

        # start tpc connector
        print('start tpc connector')
        resp = requests.post(
            'http://' + tpcconfig['debezium.connect.server'] + '/connectors', headers={'content-type': 'application/json'}, data=json.dumps(config), verify=False)
        print(resp.content)
        print(resp.status_code)
        retvalue = json.loads(resp.content)
        print(retvalue)

        while resp.status_code > 200:
            print('get status tpc connector')
            resp = requests.get(
                'http://' + tpcconfig['debezium.connect.server'] + '/connectors/tpc-connector/status',  verify=False)
            print(resp.content)
            print(resp.status_code)
            retvalue = json.loads(resp.content)
            print(retvalue)
            time.sleep(1)

    conn.jconn.setAutoCommit(False)
    for x in range(len(tpcconfig['tpc']['commit.intervals'])):
        print(tpcconfig['tpc']['commit.intervals'][x])
        curs = conn.cursor()
        for y in range(int(tpcconfig['tpc']['count'])):
            curs.execute(tpcconfig['sql']['insert'])
            if ((y % (tpcconfig['tpc']['commit.intervals'][x])) == (tpcconfig['tpc']['commit.intervals'][x] - 1)):
                conn.commit()
        conn.commit()
        topicexport(bootstrapserver, topicname, int(
            tpcconfig['tpc']['count']), tpcconfig['tpc']['commit.intervals'][x])

        kafkaadmin.delete_topics([topicname], 30)
        print('Wait 30 second for TOPIC clean up')
        time.sleep(30)


if __name__ == "__main__":
    main(sys.argv[1:])
