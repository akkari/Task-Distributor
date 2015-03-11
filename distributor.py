#!/usr/bin/python
# -*- coding:utf-8 -*-
# Created on 2014/09/10

import os
import threading
import socket
import itertools
import shutil
import time
import Queue
import sys
import getopt
import re
import tarfile
from contextlib import closing
import struct
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


class host_handler(threading.Thread):
# Communication Codes
    CHECK_ALIVE = 1
    SEND_FILE = 2
    ALIVE = 3
    READY_2_RECV = 4

    def __init__(self, host_no, host, sub_system, incoming_socks, thread_lock):
        self.host_no = host_no
        self.host = host
        self.sub_system = sub_system
        self.incoming_socks = incoming_socks
        self.lock = thread_lock
        threading.Thread.__init__(self)

    def send_file(self, filename, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.send(struct.pack('I', self.SEND_FILE))
        data = struct.unpack('I', sock.recv(4))[0]
        if data and data == self.READY_2_RECV:
            with closing(sock.makefile('w')) as f_out_socket:
                with open(filename, 'rb') as f_in_from_disk:
                    shutil.copyfileobj(f_in_from_disk, f_out_socket)


    def recv_file(self, incoming_sock, target_dir, file_name):
        incoming_sock.send(struct.pack('I', self.READY_2_RECV))
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
        with closing (incoming_sock.makefile('r')) as f_in_socket:
            with open(os.path.join(target_dir, file_name), 'wb') as f_out_to_disk:
                shutil.copyfileobj(f_in_socket, f_out_to_disk)


    def compress(self, i, o):
        filenames = ' '.join(os.path.split(x)[-1] for x in i)
        directory = os.path.split(i[0])[0]
        directory = directory if directory else '.'
        cmd = 'tar zcf %s --directory=%s %s' % (o, directory, filenames)
        os.system(cmd)

    def run(self):
    # compress files
        i, o = self.sub_system
        self.compress(i, o)
    # send files
        host, port = self.host.split(':')
        port = int(port, base=10)
        self.send_file(os.path.join('to_be_distributed', 'sys-%d.tar.gz' % (self.host_no,)), host, port)
    # wait for reply
        while True:
            with self.lock:
                if self.incoming_socks[self.host_no-1]:
                    incoming_sock = self.incoming_socks[self.host_no-1]
                    break
        data = struct.unpack('I', incoming_sock.recv(4))[0]
        if data == self.SEND_FILE:
            self.recv_file(incoming_sock, 'files_gathered', '.'.join(['out'+str(self.host_no), 'tar', 'gz']))
            incoming_sock.close()

        in_tar = tarfile.open('files_gathered/out%d.tar.gz' % self.host_no, 'r')
        in_tar.extractall('.')
        in_tar.close()

class Distributor:
# Port numbers
    default_client_listen_port = 60000
    default_server_listen_port = 60001
# Communication codes
    CHECK_ALIVE = 1
    SEND_FILE = 2
    ALIVE = 3
    READY_2_RECV = 4
    def __init__(self, hosts, files, template, port):
        self.hosts = [ x if re.search(r'^.+\:[1-9]\d*$', x) else '%s:%s' % (x, self.default_client_listen_port) for x in hosts ]
        self.files = files
        self.template = template
        if port:
            self.port = int(port)
        else:
            self.port = self.default_server_listen_port

    def start(self):
        if self.check_files() and self.check_hosts() and self.check_template():
            self.copy_files()
            self.pack_files()
            self.host_threads = []
            incoming_socks = [None] * len(self.hosts)
            thread_lock = threading.Lock()
            for i in xrange(len(self.hosts)):
                thread = host_handler(i+1, self.hosts[i], self.sub_systems[i], incoming_socks, thread_lock)
                thread.start()
                self.host_threads.append(thread)
            #Listen for results
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('', self.port))
            sock.listen(10)
            while not all(incoming_socks):
                incoming_sock, incoming_addr = sock.accept()
                sys_no = struct.unpack('I', (incoming_sock.recv(4)))[0]
                incoming_socks[sys_no-1] = incoming_sock

        else:
            sys.exit(1)

    def check_hosts(self):
        alive = False
        hosts =self.hosts
        if hosts:
            bad_hosts = []
            for host in hosts:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                host, port = host.split(':')
                port = int(port, base=10)
                try:
                    sock.connect((host, port))
                    sock.send(struct.pack('I', (self.CHECK_ALIVE)))
                    data = struct.unpack('I', sock.recv(4))[0]
                    if not data == self.ALIVE:
                        bad_hosts.append(host)
                except socket.error:
                    bad_hosts.append('%s:%s' %(host, port))
            if bad_hosts:
                print 'Failed to connect to \n%s.' % '\n'.join(bad_hosts)
            else:
                alive = True
        else:
            print 'Please specify the hosts you want to connect to.'
        return alive

    def check_files(self):
        files = self.files
        if files:
            bad_files = []
            for f in files:
                if not os.path.exists(f):
                    bad_files.append(f)
            if bad_files:
                print 'File \n%s\ncan not be found.' % '\n'.join(bad_files)
                return False
            else:
                return True
        else:
            print 'Please specify the files that you want to distribute.'
            return False


    def check_template(self):
        template = self.template
        if template:
            if os.path.exists(template):
                return True
            else:
                print 'File "%s" does not exist.' % (template,)
                return False
        else:
            print 'Please specify the template file for your work.'
            return False

    def copy_files(self):
        old_file_names = self.files
        new_file_names = map(lambda x: os.path.split(x)[-1], old_file_names)
        new_file_names = map(lambda x: os.path.join('to_be_distributed', x), new_file_names)

        if os.path.exists('to_be_distributed'):
            shutil.rmtree('to_be_distributed')
        os.mkdir('to_be_distributed')

        for src, dst in itertools.izip(old_file_names, new_file_names):
            shutil.copyfile(src, dst)

    def pack_files(self):
        n_sys = len(self.hosts)
        template_name = self.template

        file_names = map(lambda x: os.path.split(x)[-1], self.files) # file names without preceding path
        pattern = r'([0-9]+)\.'
        file_names.sort(key = lambda x: int(re.findall(pattern, x)[-1])) # Sort file names by their numbers.
        file_numbers = [] # Extract the number of files to a list.
        for name in file_names:
            number = re.findall(pattern, name)[-1]
            if number not in file_numbers:
                file_numbers.append(number)
        pattern = r'(.*\D)\d+\.'
        pure_name = re.search(pattern, file_names[0]).groups()[0]

        self.sub_systems = []
        for sys_no in xrange(1, 1+n_sys):
            sub_system_numbers = [file_numbers[x] for x in xrange(sys_no-1, len(file_numbers), n_sys)]
            pattern = '|'.join( "\D%s\.\w+$|^%s\.\w+$" % (x,x) for x in sub_system_numbers)
            sub_filenames = filter(lambda x: re.search(pattern, x), file_names)

        # generate xml
            tree = ET.parse(template_name)
            task = tree.getroot()
            tag_port = ET.SubElement(task, "port")
            tag_port.text = str(self.port)
            tag_filename = ET.SubElement(task, "filename")
            tag_filename.text = pure_name
            tag_host_no = ET.SubElement(task, "host_no")
            tag_host_no.text = str(sys_no)
            tag_sub_sys_numbers = ET.SubElement(task, "sub_sys_numbers")
            tag_sub_sys_numbers.text = ' '.join(map(str, sub_system_numbers))
            xml_text = ET.tostring(task)
            with open('to_be_distributed/task%s.xml' % sys_no, 'w') as f:
                f.write(xml_text)

        # pack files
            ins = sub_filenames + ['task%d.xml' % sys_no]
            ins = [ os.path.join('to_be_distributed', x) for x in ins ]
            out = os.path.join('to_be_distributed', 'sys-%d.tar.gz' % sys_no)
            self.sub_systems.append((ins, out))

    def configure(self, hosts=None, files=None, template=None, port=None):
        if hosts:
            self.hosts = [ x if re.search(r'^.+\:[1-9]\d*$', x) else '%s:%s' % (x, self.default_client_listen_port) for x in hosts ]
            print self.hosts
        if files:
            self.files = files
        if template:
            self.template = template
        if port:
            self.port = port

def main():
    hosts, files, template = ['']*3
    port = None

    opts, args = getopt.getopt(sys.argv[1:], 'f:F:h:H:t:p:')
    for opt, arg in opts:
        if opt == '-f':
            files = arg.split()
        elif opt == '-p':
            port = arg
        elif opt == '-F':
            with open(arg, 'r') as f:
                files = map(lambda x: x.strip(), f.readlines())
        elif opt == '-h':
            hosts = arg.split()
        elif opt == '-H':
            with open(arg, 'r') as f:
                hosts = map(lambda x: x.strip(), f.readlines())
        elif opt == '-t':
            template = arg

    distributor = Distributor(hosts, files, template, port)
    distributor.start()
    while True:
        if all( not x.is_alive() for x in distributor.host_threads ):
            break
        time.sleep(0.1)
    print 'Program terminated.'

if __name__=="__main__":main()

