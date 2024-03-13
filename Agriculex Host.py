#!/usr/bin/env python3

import sys
import socket
import selectors
import types
import time
import mysql.connector

sel = selectors.DefaultSelector()


host="10.1.175.40"
user="iotadmin"
password="controlsadmin"
database="CSH Production"


#Example
""" 
Echoing b'pack:   count: 400\n' to ('10.35.1.62', 45170)
Echoing b'pack: PED00053460_3  count: 250\n' to ('10.35.1.60', 52464)
Echoing b'pack:  ount: 400\n' to ('10.35.1.62', 45170)
Echoing b'pack: PED00053460_4  count: 250\n' to ('10.35.1.60', 52464)
Echoing b'pack: PED00053460_5  count: 250\n' to ('10.35.1.60', 52464)
Echoing b'pack: PED00053460_6  count: 250\n' to ('10.35.1.60', 52464)
Echoing b'pack:   count: 400\n' to ('10.35.1.62', 45170)
Echoing b'pack: PED00053460_7  count: 250\n' to ('10.35.1.60', 52464) 

Echoing b'dump: 0\n\n' to ('10.35.1.60', 40790)
Echoing b'dump: 0\n\n' to ('10.35.1.60', 40790)
Echoing b'pack: 7659367_10279  count: 250\n' to ('10.35.1.60', 40790)
Echoing b'pack: I466574  count: 250\n' to ('10.35.1.60', 40790)
Echoing b'pack: 7669085_11677  count: 250\n' to ('10.35.1.60', 40790)

"""

#{data.outb!r} = pack: PED00053460_7  count: 250\n
#{data.addr} = ('10.35.1.60', 52464)

def parseData(data):
    outputString = data.outb 
    machine_ip = data.addr[0]

    outputString = outputString.strip("\n")
    outputString = outputString.strip(":")
    outputList = outputString.split(" ")

    action = outputList[0]

    if action == "dump":
        plant_name = ""
        seed_count = outputList[1]

    if action == "pack":
        if len(outputList) == 4:
            plant_name = outputList[1]
            seed_count = outputList[3]
        if len(outputList) == 3:
            plant_name = ""
            seed_count = outputList[2]

    datalist = (action, plant_name, seed_count, machine_ip)

    return datalist

def writeToDb(data):
    try:
        datalist = parseData(data)
    except:
        print("parse data failed")

    mydb = mysql.connector.connect(
        host,
        user,
        password,
        database
    )

    mycursor = mydb.cursor()

    sql = "INSERT INTO Agriculex_Raw (`Action`, `Time`, `Plant Name`, Count, `I.P.`) VALUES (%s, %s, %s, %i, %s)" 
    val = (datalist[0], time.time(),  datalist[1], datalist[2], datalist[3])

    mycursor.execute(sql, val)

    mydb.commit
    mydb.close

def accept_wrapper(sock):
    conn, addr = sock.accept()  # Should be ready to read
    print(f"Accepted connection from {addr}")
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)


def service_connection(key, mask):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            data.outb += recv_data
            try:
                writeToDb(data)
            except:
                print("error in write to Db")


        else:
            print(f"Closing connection to {data.addr}")
            sel.unregister(sock)
            sock.close()
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            print(f"Echoing {data.outb!r} to {data.addr}")
            sent = sock.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]


if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <host> <port>")
    sys.exit(1)

host, port = sys.argv[1], int(sys.argv[2])
lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.bind((host, port))
lsock.listen()
print(f"Listening on {(host, port)}")
lsock.setblocking(False)
sel.register(lsock, selectors.EVENT_READ, data=None)

try:
    while True:
        events = sel.select(timeout=None)
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key, mask)
except KeyboardInterrupt:
    print("Caught keyboard interrupt, exiting")
finally:
    sel.close()
