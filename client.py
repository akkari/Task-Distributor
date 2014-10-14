#!/usr/bin/python
# -*- coding:utf-8 -*-
# Created on 2014/09/12

import socket
import time
import thread
import os
import sys

# Communication codes
CHECK_ALIVE = 1
SEND_FILE = 2
ALIVE = 3
READY_2_RECV = 4

# Port numbers
default_client_listen_port = 60000
server_listen_port_base = 60000

def now():
    t = time.localtime()
    t = tuple(map(str, (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)))
    return '%s.%s.%s_%s.%s.%s' % t

def process(target_dir, addr):
    decompress = 'tar xzf %s/in.tar.gz -C %s' % (target_dir, target_dir)
    os.system(decompress)
    script = filter(lambda x: x.split('.')[-1] == 'sh', os.listdir(target_dir))[0]
    host_no = int(script.split('.')[0])
    run_script = 'bash %s/%s %s' % (target_dir, script, target_dir)
    os.system(run_script)

    send_file('%s/out.tar.gz' % (target_dir,), addr, server_listen_port_base+host_no)

def recv_file(incoming_sock, target_dir, file_name):
    incoming_sock.send(str(READY_2_RECV))
    os.mkdir(target_dir)
    with open(os.path.join(target_dir, file_name), 'wb') as f:
        while True:
            data = incoming_sock.recv(1024)
            if not data: break
            f.write(data)

def send_file(file_name, host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host,port))
    sock.send(str(SEND_FILE))
    data = sock.recv(1024)
    if data and int(data) == READY_2_RECV:
        with open(file_name, 'rb') as f:
            while True:
                bytes = f.read(1024)
                if not bytes: break
                sent = sock.send(bytes)
                assert sent == len(bytes)
            sock.close()

def main():
    args = sys.argv
    if len(args) > 1:
        client_listen_port = int(args[1], base=10)
    else:
        client_listen_port = default_client_listen_port
    host = ''
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.bind((host, client_listen_port))
    listen_sock.listen(5)
    while True:
        incoming_sock, incoming_addr = listen_sock.accept()
        print 'Connected by', incoming_addr, 'at', now()
        data = incoming_sock.recv(1024)
        if not data: continue
        if int(data) == CHECK_ALIVE:
            incoming_sock.send(str(ALIVE))
        elif int(data) == SEND_FILE:
            cur_time = now()
            recv_file(incoming_sock, cur_time, 'in.tar.gz')
            thread.start_new_thread(process, (cur_time, incoming_addr[0]))
        incoming_sock.close()

if __name__=="__main__":main()

