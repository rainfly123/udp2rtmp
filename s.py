#!/usr/bin/env python
import select
import socket
  
inputs = list()
excepts = list()
timeout = 20

def OpenServer():
    server = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    server.setblocking(False)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR  , 1)
    server_address = ('0.0.0.0', 1234)
    server.bind(server_address)
    return server
  
inputs.append(OpenServer())

while True:
    excepts = inputs
    readable , writable , exceptional = select.select(inputs, [], excepts, timeout)
  
    if not (readable or exceptional) :
        continue;   

    for s in readable :
       data = s.recv(1316)
       if data :
           print data
       else:
           inputs.remove(s)
           s.close()
           inputs.remove(s)
           inputs.append(OpenServer())

    for s in exceptional:
        s.close()
        inputs.remove(s)
        inputs.append(OpenServer())

