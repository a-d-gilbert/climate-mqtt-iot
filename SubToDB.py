import paho.mqtt.client as mqtt
import time
import psycopg2 as pg
import json

credsfile = 'creds.json'
configfile= 'config.json'

def get_connection(credsfile, configfile):
    uname = ''
    psk = ''
    db = ''
    hname = ''

    with open(credsfile) as cf:
        creds = json.load(cf)
        uname = creds['username']
        psk = creds['password']

    with open(configfile) as cf:
        config = json.load(cf)
        hname = config['hostname']
        db = config['database']

    conn = pg.connect(host=hname, database=db, user=uname, password=psk)

    return conn

def on_message(client, data, message):

    sql = "INSERT INTO climatereadings(location,temperature,humidity) VALUES(%s,%s,%s) RETURNING readingid;"
    msg = str(message.payload.decide('utf-8'))
    climate = json.loads(msg)

    try:
        conn = get_connection(credsfile, configfile)
        cur = conn.cursor()
        cur.execute(sql, (climate['location'], climate['temperature'], climate['humidity']))
        rid = cur.fetchone()[0]
        conn.commit()
        cur.close()
        print('commited reading:' + rid + climate)
        
    except (Exception, pg.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

client =  mqtt.Client('climate_sub')
client.on_message = on_message

client.connect('localhost', 1883)

while True:
    client.loop_start()
    client.subscribe('house/temp/lr')
    client.loop_stop()