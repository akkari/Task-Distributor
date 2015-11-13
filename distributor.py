#!/usr/bin/python
# -*- coding:utf-8 -*-
# Created on 2014/09/10

import os
import threading
import socket
import itertools
import shutil
import time
import sys
import getopt
import re
from contextlib import closing
import struct
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET


class task_handler(threading.Thread):
# Communication Codes
    CHECK_ALIVE = 1
    SEND_FILE = 2
    ALIVE = 3
    READY_2_RECV = 4

    def __init__(self, task_config, incoming_socks, thread_lock):
        self.task_no = task_config['task_no']
        self.host = task_config['host']
        self.template = task_config['template']
        self.files_to_compress = task_config['files_to_compress']
        self.tar_file = task_config['tar_file']
        self.xml_tag = task_config['xml_tag']
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
        CMD_LENGTH_LIMIT = 50000
        directory = os.path.split(i[0])[0]
        directory = directory if directory else '.'
        left, right = 0, 0
        while right < len(i):
            right = min(left + 1000, len(i))
            filenames = ' '.join(os.path.split(x)[-1] for x in i[left:right])
            cmd = 'tar f %s --append --directory=%s %s' % (o, directory, filenames)
            while len(cmd) > CMD_LENGTH_LIMIT:
                right -= 1
                filenames = ' '.join(os.path.split(x)[-1] for x in i[left:right])
                cmd = 'tar f %s --append --directory=%s %s' % (o, directory, filenames)
            os.system(cmd)
            left = right


    def generate_xml(self):
    # generate xml
        tree = ET.parse(self.template)
        task = tree.getroot()
        for key, value in self.xml_tag.items():
            tag = ET.SubElement(task, key)
            tag.text = str(value)
        xml_text = ET.tostring(task)
        with open('to_be_distributed/task%s.xml' % self.task_no, 'w') as f:
            f.write(xml_text)

    def run(self):
    # Do nothing if no files to be distributed.
        if not self.files_to_compress:
            return
    # Generate xml file.
        self.generate_xml()
    # compress files
        self.compress(self.files_to_compress, self.tar_file)
    # send files
        host, port = self.host.split(':')
        port = int(port, base=10)
        self.send_file(os.path.join('to_be_distributed', 'task-%d.tar' % (self.task_no,)), host, port)
    # wait for reply
        while True:
            with self.lock:
                if self.incoming_socks[self.task_no-1]:
                    incoming_sock = self.incoming_socks[self.task_no - 1]
                    break
        data = struct.unpack('I', incoming_sock.recv(4))[0]
        if data == self.SEND_FILE:
            self.recv_file(incoming_sock, 'files_gathered', '.'.join(['out' + str(self.task_no), 'tar']))
            incoming_sock.close()

        cmd = 'tar xf files_gathered/out%d.tar --warning=no-timestamp' % self.task_no
        os.system(cmd)

class Distributor:
# Port numbers
    default_client_listen_port = 60000
    default_server_listen_port = 60001
# Communication codes
    CHECK_ALIVE = 1
    SEND_FILE = 2
    ALIVE = 3
    READY_2_RECV = 4
    def __init__(self, hosts, files, template, port, sort_names=False):
        self.hosts = [ x if re.search(r'^.+\:[1-9]\d*$', x) else '%s:%s' % (x, self.default_client_listen_port) for x in hosts ]
        self.files = files
        self.template = template
        if port:
            self.port = int(port)
        else:
            self.port = self.default_server_listen_port
        self.sort_names = sort_names

    def start(self):
        if self.check_files() and self.check_hosts() and self.check_template():
            # Establish a socket and bind it to the designated port.
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(('', self.port))
                sock.listen(10)
            except socket.error:
                print "Port %s already in use." % self.port
                sys.exit(1)

            self.copy_files()
            self.assign_tasks()

            self.host_threads = []
            incoming_socks = [None] * min(len(self.files), len(self.hosts))
            thread_lock = threading.Lock()
            for i in xrange(len(incoming_socks)):
                thread = task_handler(self.task_configs[i], incoming_socks, thread_lock)
                thread.start()
                self.host_threads.append(thread)

            #Listen for results
            while not all(incoming_socks):
                incoming_sock, incoming_addr = sock.accept()
                task_no = struct.unpack('I', (incoming_sock.recv(4)))[0]
                incoming_socks[task_no-1] = incoming_sock

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

    def assign_tasks(self):
        n_tasks = len(self.hosts)

        file_names = map(lambda x: os.path.split(x)[-1], self.files) # file names without preceding path
        pattern = r'([0-9]+)\.'
        if self.sort_names:
            file_names.sort(key = lambda x: int(re.findall(pattern, x)[-1])) # Sort file names by the value of their numbers.

        # Determine the number of types of files.
        suffix_no = 0
        pre_number = re.findall(pattern, file_names[0])[-1]
        for name in file_names:
            cur_number = re.findall(pattern, name)[-1]
            if cur_number != pre_number:
                break
            suffix_no += 1
            pre_number = cur_number

        # Extract the number of files to a list.
        file_numbers = [re.findall(pattern, file_names[x])[-1] for x in xrange(0, len(file_names), suffix_no)]

        pattern = r'(.*\D)\d+\.'
        pure_name = re.search(pattern, file_names[0]).groups()[0]

        self.task_configs = []
        for task_no in xrange(n_tasks):
            task_config = {}
            task_config['task_no'] = task_no + 1
            task_config['host'] = self.hosts[task_no]
            task_config['template'] = self.template

            task_config['files_to_compress'] = []
            for _x in xrange(task_no * suffix_no, len(file_names), n_tasks * suffix_no):
                task_config['files_to_compress'].extend([file_names[_x+i] for i in xrange(suffix_no)])

            task_config['files_to_compress'] += ['task%d.xml' % (task_no + 1)]
            task_config['files_to_compress'] = [os.path.join('to_be_distributed', x) for x in task_config['files_to_compress']]
            task_config['tar_file'] = "to_be_distributed/task-%d.tar" % (task_no + 1)

            xml_tag = {}
            xml_tag['port'] = str(self.port)
            xml_tag['pure_name'] = pure_name
            xml_tag['task_no'] = str(task_no + 1)
            xml_tag['file_numbers'] = ' '.join(file_numbers[_x] for _x in xrange(task_no, len(file_numbers), n_tasks))
            task_config['xml_tag'] = xml_tag

            self.task_configs.append(task_config)


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
    sort_names = False

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
        elif opt == '-s':
            sort_names = True

    distributor = Distributor(hosts, files, template, port, sort_names)
    distributor.start()
    while True:
        if all( not x.is_alive() for x in distributor.host_threads ):
            break
        time.sleep(0.1)
    print 'Program terminated.'

if __name__=="__main__":main()

