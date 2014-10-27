#!/usr/bin/python
# -*- coding:utf-8 -*-
# Created on 2014/09/12

import socket
import time
import thread
import threading
import os
import sys
import shutil
import fnmatch
import re
from contextlib import closing
import struct
import glob
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

# Communication codes
CHECK_ALIVE = 1
SEND_FILE = 2
ALIVE = 3
READY_2_RECV = 4

# Port numbers
default_client_listen_port = 60000

class multi_task(threading.Thread):
    threads_alive = 0
    def __init__(self, cmd, working_path, thread_lock):
        self.cmd = cmd
        self.working_path = working_path
        self.lock = thread_lock
        threading.Thread.__init__(self)

    def run(self):
        with self.lock:
            multi_task.threads_alive += 1

        cd ='cd %s' % self.working_path
        os.system(';'.join([cd, self.cmd]))

        with self.lock:
            multi_task.threads_alive -= 1

def path_filter(directory, pattern, type):
    file_names = filter(os.path.isfile, [os.path.join(directory, x) for x in os.listdir(directory)])
    file_names = [os.path.split(x)[-1] for x in file_names]
    if type == 'wc':
        return fnmatch.filter(file_names, pattern)
    elif type == 're':
        pattern = fnmatch.translate(pattern)
        re_obj = re.compile(pattern)
        return filter(re_obj.match, file_names)


def now():
    t = time.localtime()
    t = tuple(map(str, (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)))
    return '%s.%s.%s_%s.%s.%s' % t

def process(target_dir, addr):
    decompress = 'tar xzf %s/in.tar.gz -C %s' % (target_dir, target_dir)
    os.system(decompress)

    task_file = glob.glob('%s/*.xml' % target_dir)[0]
    task = ET.ElementTree(file=task_file)
    for elem in task.getiterator():
        if elem.tag == 'command':
            cmd_template = elem.text
        elif elem.tag == 'result':
            result_pattern = elem.text
            result_pattern_type = elem.attrib['type']
        elif elem.tag == 'np':
            np = int(elem.text)
        elif elem.tag == 'host_no':
            host_no = int(elem.text)
        elif elem.tag == 'filename':
            filename = elem.text
        elif elem.tag == 'port':
            server_port = int(elem.text)
        elif elem.tag == 'sub_sys_numbers':
            sub_sys_numbers = elem.text.split()

    thread_lock = threading.Lock()
    index = 0
    while index < len(sub_sys_numbers):
        if multi_task.threads_alive < np:
            cmd = cmd_template % {'name': filename+sub_sys_numbers[index]}
            task_thread = multi_task(cmd, target_dir, thread_lock)
            task_thread.start()
            index += 1
            time.sleep(0.001)

    while multi_task.threads_alive > 0:
        continue


    files_to_gather = path_filter(target_dir, result_pattern, result_pattern_type)
    tar_cmd = 'tar zcf %s -C %s %s' % ('%s/out.tar.gz' % target_dir, target_dir,  ' '.join(files_to_gather))
    os.system(tar_cmd)


    send_file('%s/out.tar.gz' % (target_dir,), addr, server_port, host_no)

def recv_file(incoming_sock, target_dir, file_name):
    incoming_sock.send(struct.pack('I', READY_2_RECV))
    os.mkdir(target_dir)
    with closing(incoming_sock.makefile('r')) as f_in_socket:
        with open(os.path.join(target_dir, file_name), 'wb') as f_out_to_disk:
            shutil.copyfileobj(f_in_socket, f_out_to_disk)


def send_file(file_name, host, port, host_no):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host,port))
    sock.send(struct.pack('I', host_no))
    sock.send(struct.pack('I', SEND_FILE))
    data = struct.unpack('I', sock.recv(4))[0]
    if data and data == READY_2_RECV:
        with closing(sock.makefile('w')) as f_out_socket:
            with open(file_name, 'rb') as f_in_from_disk:
                shutil.copyfileobj(f_in_from_disk, f_out_socket)

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
        data = struct.unpack('I', incoming_sock.recv(4))[0]
        if not data: continue
        if data == CHECK_ALIVE:
            incoming_sock.send(struct.pack('I', ALIVE))
        elif data == SEND_FILE:
            cur_time = now()
            recv_file(incoming_sock, cur_time, 'in.tar.gz')
            thread.start_new_thread(process, (cur_time, incoming_addr[0]))
        incoming_sock.close()

if __name__=="__main__":main()

