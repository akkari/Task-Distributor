#!/usr/bin/python
# -*- coding:utf-8 -*-
# Created on 2014/09/10

try:
    from Tkinter import *
    import tkFileDialog
    import tkSimpleDialog
    import tkMessageBox
except ImportError:
    print 'No Tkinter support on your system.'
import os
import thread
import socket
import itertools
import shutil
import time as Time
import Queue
import sys
import getopt
import re
import tarfile

class Distributor:
    def __init__(self, parent=None, mode='gui'):
        """ Set some useful constants and the mode of the run, which defaults to 'gui'.
        When in 'gui' mode, all the GUI widgets needed will be initialized. When in 'cmd_line'
        mode, nothing else will be done.
            In order for the instance to run, the 'configure' method must be called explicitly to
        fill out the information about the job, such as hosts to connect to and files to be distributed.
        """
    # Port numbers
        self.client_listen_port = 60000 # default
        self.server_listen_port_base = 60000
    # Communication codes
        self.CHECK_ALIVE = 1
        self.SEND_FILE = 2
        self.ALIVE = 3
        self.READY_2_RECV = 4
    # Mode of the run
        self.mode = mode

        if mode == 'cmd_line':
            pass

        elif mode == 'gui':
# GUI mode
            self.parent = parent
    # Frames
            # host_frame
            self.host_frame = Frame(self.parent, borderwidth=3, relief=RIDGE)
            self.host_frame.pack(side=TOP, pady=5, fill=X, expand=YES)

            # file_frame
            self.file_frame = Frame(self.parent, borderwidth=3, relief=RIDGE)
            self.file_frame.pack(side=TOP, pady=5, fill=X, expand=YES)

            # template_frame
            self.template_frame = Frame(self.parent, borderwidth=3, relief=RIDGE)
            self.template_frame.pack(side=TOP, pady=5, fill=X, expand=YES)

            # main_control_frame
            self.main_control_frame = Frame(self.parent)
            self.main_control_frame.pack(side=TOP, anchor=E)

    # In-frame widgets
            # host_frame
            self.l_host = Label(self.host_frame, text='Specify the hosts you want to connect to.', justify=LEFT)
            self.hosts = Listbox(self.host_frame, bg='white')
            self.host = Entry(self.host_frame)
            self.host.bind('<Return>', lambda event: self.add_host(event, self.host.get()))
            self.b_add_host = Button(self.host_frame, text='add', command=lambda : self.add_host(None, self.host.get()))
            self.b_edit = Button(self.host_frame, text='edit', command=self.edit)
            self.b_delete_host = Button(self.host_frame, text='delete', command=lambda :self.delete('host'))
            self.b_clear_host = Button(self.host_frame, text='clear', command=lambda :self.clear('host'))
            self.b_load = Button(self.host_frame, text='load...', command=self.load)
            self.b_save = Button(self.host_frame, text='save', command=self.save)

            self.l_host.grid(row=0, column=0, columnspan=4, sticky=W)
            self.b_load.grid(row=1, column=0)
            self.b_save.grid(row=1, column=1)
            self.b_delete_host.grid(row=1, column=2)
            self.b_clear_host.grid(row=1, column=3)
            self.hosts.grid(row=2, column=0, columnspan=4, sticky=W+E)
            self.host.grid(row=3, column=0, columnspan=4, sticky=W+E)
            self.b_add_host.grid(row=3, column=4)
            self.b_edit.grid(row=3,column=5)

            # file_frame
            self.l_file = Label(self.file_frame, text='Specify the files you want to distribute.', justify=LEFT)
            self.files = Listbox(self.file_frame, bg='white')
            self.b_openfiles = Button(self.file_frame, text='open files...', command=self.openfiles)
            self.b_openfolder = Button(self.file_frame, text='open folder...', command=self.openfolder)
            self.b_delete_file = Button(self.file_frame, text='delete', command=lambda :self.delete('file'))
            self.b_clear_file = Button(self.file_frame, text='clear', command=lambda :self.clear('file'))

            self.l_file.grid(row=0, column=0, columnspan=4, sticky=W)
            self.b_openfiles.grid(row=1, column=0)
            self.b_openfolder.grid(row=1, column=1)
            self.b_delete_file.grid(row=1, column=2)
            self.b_clear_file.grid(row=1, column=3)
            self.files.grid(row=2, column=0, columnspan=4, sticky=W+E)

            # template_frame
            self.l_template = Label(self.template_frame, text='Specify the script template for your work.', justify=LEFT)
            self.template = Entry(self.template_frame)
            self.b_open_template = Button(self.template_frame, text='open file...', command=self.open_template_file)

            self.l_template.grid(row=0, column=0, sticky=W)
            self.template.grid(row=1, column=0, columnspan=2, sticky=W+E)
            self.b_open_template.grid(row=1, column=2)


            # main_control_frame
            self.b_start = Button(self.main_control_frame, text='start', command=self.start)
            self.b_quit = Button(self.main_control_frame, text='quit', command=self.quit)

            self.b_start.grid(row=0, column=0)
            self.b_quit.grid(row=0, column=1)




    def add_host(self, event, host):
        if host:
            host = host if re.search(r'^\w+\:[1-9]\d*$', host) else '%s:%s' % (host, self.client_listen_port)
            if host not in self.hosts.get(0, END):
                self.hosts.insert(END, host)
            else:
                tkMessageBox.showwarning('Warning', 'Host "%s" already added.' % self.host.get())
            self.host.select_range(0, END)

    def edit(self):
        if self.hosts.curselection():
            cur_line = self.hosts.curselection()[0]
            host = tkSimpleDialog.askstring('Enter a new host.', 'new host')
            if host:
                self.hosts.delete(cur_line)
                self.hosts.insert(cur_line, host)


    def save(self):
        hosts = self.hosts.get(0, END)
        if hosts:
            host_file = tkFileDialog.asksaveasfile(mode='w')
            for line in hosts:
                host_file.write(line+'\n')

    def load(self):
        host_file = tkFileDialog.askopenfile(mode='r')
        if host_file:
            hosts = map(lambda obj:obj.rstrip(), host_file.readlines())
            self.hosts.delete(0, END)
            self.hosts.insert(END, *hosts)

    def delete(self, what):
        if what == 'host':
            if self.hosts.curselection():
                self.hosts.delete(self.hosts.curselection()[0])
        elif what == 'file':
            if self.files.curselection():
                self.files.delete(self.files.curselection()[0])

    def clear(self, what):
        if what == 'host':
            self.hosts.delete(0, END)
        elif what == 'file':
            self.files.delete(0, END)


    def open_template_file(self):
        filename = tkFileDialog.askopenfilename()
        if filename:
            self.template.delete(0, END)
            self.template.insert(0, filename)

    def openfiles(self):
        filenames = tkFileDialog.askopenfilenames()
        if filenames:
            self.files.insert(END, *filenames)

    def openfolder(self):
        folder = tkFileDialog.askdirectory()
        if folder:
            filenames = filter(lambda x: not os.path.isdir(x) and os.path.split(x)[-1][0] != '.', [ os.path.join(folder, x) for x in os.listdir(folder)])
            if filenames:
                self.files.insert(END, *filenames)

    def start(self):
        if self.check_files() and self.check_hosts() and self.check_template():
            self.copy_files()
            self.pack_files()
            if self.mode == 'gui':
                self.open_session()
                for i in xrange(len(self.hosts.get(0, END))):
                    thread.start_new_thread(self.host_handler, (i+1,))
            elif self.mode == 'cmd_line':
                for i in xrange(len(self.hosts)):
                    thread.start_new_thread(self.host_handler, (i+1,))
                    self.exit_mutexes = [ thread.allocate_lock() for x in xrange(len(self.hosts)) ]


    def update_info(self, status, start_time):
        while True:
            try:
                data = self.data_queue.get(block=False)
                host_no, s, t = data['host_no'], data['status'], data['time']
                status[host_no-1].append(s)
                if s == 'finished':
                    start_time[host_no-1] = Time.ctime(Time.time()-start_time[host_no-1]).split()[3]
                if t:
                    start_time[host_no-1] = t
            except Queue.Empty:
                break
        time_display = [ Time.ctime(Time.time()-x).split()[3] if x and status[i][-1] != 'finished' else x for (i,x) in enumerate(start_time) ]
        status_display = map(lambda x: '==>'.join(x), status)
        self.rs_lb_time.delete(0, END)
        self.rs_lb_time.insert(0, *time_display)
        self.rs_lb_status.delete(0, END)
        self.rs_lb_status.insert(0, *status_display)

        self.running_session.after(1000, lambda : self.update_info(status, start_time))

    def host_handler(self, host_no):
        if self.mode == 'gui':
    #       *正在发送->发送完毕->等待->收到结果
            data_2_queue = {'host_no':host_no, 'status':'sending files', 'time':Time.time()}
            self.data_queue.put(data_2_queue)

            host, port = self.hosts.get(0, END)[host_no-1].split(':')
            port = int(port, base=10)
            self.send_file(os.path.join('to_be_distributed', 'sys-%d.tar.gz' % (host_no,)), host, port)

    #       正在发送->*发送完毕->等待->收到结果
            data_2_queue = {'host_no':host_no, 'status':'files sent', 'time':''}
            self.data_queue.put(data_2_queue)

    #       正在发送->发送完毕->*等待->收到结果
            data_2_queue = {'host_no':host_no, 'status':'waiting for reply', 'time':''}
            self.data_queue.put(data_2_queue)


            listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.bind(('', self.server_listen_port_base+host_no))
            listen_sock.listen(5)
            while True:
                incoming_sock, incoming_addr = listen_sock.accept()
                print 'Connected by', incoming_addr, 'at', Time.asctime()
                data = incoming_sock.recv(1024)
                if int(data) == self.SEND_FILE:
                    self.recv_file(incoming_sock, 'files_gathered', '.'.join(['out'+str(host_no), 'tar', 'gz']))
                    incoming_sock.close()
                    break
            listen_sock.close()

    #       正在发送->发送完毕->等待->*收到结果
            in_tar = tarfile.open('files_gathered/out%d.tar.gz' % host_no, 'r')
            in_tar.extractall('.')
            in_tar.close()
            data_2_queue = {'host_no':host_no, 'status':'finished', 'time':''}
            self.data_queue.put(data_2_queue)

        elif self.mode == 'cmd_line':
        # send files
            host, port = self.hosts[host_no-1].split(':')
            port = int(port, base=10)
            self.send_file(os.path.join('to_be_distributed', 'sys-%d.tar.gz' % (host_no,)), host, port)
        # wait for reply
            listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.bind(('', self.server_listen_port_base+host_no))
            listen_sock.listen(5)
            while True:
                incoming_sock, incoming_addr = listen_sock.accept()
                print 'Connected by', incoming_addr, 'at', Time.asctime()
                data = incoming_sock.recv(1024)
                if int(data) == self.SEND_FILE:
                    self.recv_file(incoming_sock, 'files_gathered', '.'.join(['out'+str(host_no), 'tar', 'gz']))
                    incoming_sock.close()
                    break
            listen_sock.close()
            in_tar = tarfile.open('files_gathered/out%d.tar.gz' % host_no, 'r')
            in_tar.extractall('.')
            in_tar.close()
            self.exit_mutexes[host_no-1].acquire()

    def open_session(self):
        # running_session => Toplevel, rs => PanedWindow in running_session
        self.running_session = Toplevel()
        self.running_session.grab_set()
        self.rs = PanedWindow(master=self.running_session, orient=HORIZONTAL, sashrelief=RIDGE, sashpad=2)
        self.rs.pack(fill=BOTH, expand=1)

        self.rs_hosts_frame = Frame(self.rs)
        self.rs_status_frame = Frame(self.rs)
        self.rs_time_frame = Frame(self.rs)

        self.rs.add(self.rs_hosts_frame)
        self.rs.add(self.rs_status_frame, width='105m')
        self.rs.add(self.rs_time_frame)

        self.rs_l_hosts = Label(self.rs_hosts_frame, text='hosts')
        self.rs_l_status = Label(self.rs_status_frame, text='status')
        self.rs_l_time = Label(self.rs_time_frame, text='time')

        self.rs_l_hosts.pack()
        self.rs_l_status.pack()
        self.rs_l_time.pack()

        self.rs_lb_hosts = Listbox(self.rs_hosts_frame)
        self.rs_lb_status = Listbox(self.rs_status_frame)
        self.rs_lb_time = Listbox(self.rs_time_frame)

        self.rs_lb_hosts.pack(fill=BOTH, expand=YES)
        self.rs_lb_status.pack(fill=BOTH, expand=YES)
        self.rs_lb_time.pack(fill=BOTH, expand=YES)

        self.rs_lb_hosts.insert(0, *self.hosts.get(0, END))
        self.data_queue = Queue.Queue()
        n_host = len(self.hosts.get(0, END))
        self.update_info([ [] for x in xrange(n_host)], ['']*n_host)



    def send_file(self, filename, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.send(str(self.SEND_FILE))
        data = sock.recv(1024)
        if data and int(data) == self.READY_2_RECV:
            with open(filename, 'rb') as f:
                while True:
                    bytes = f.read(1024)
                    if not bytes: break
                    sent = sock.send(bytes)
                    assert sent == len(bytes)
            sock.close()


    def recv_file(self, incoming_sock, target_dir, file_name):
        incoming_sock.send(str(self.READY_2_RECV))
        if not os.path.exists(target_dir):
            os.mkdir(target_dir)
        with open(os.path.join(target_dir, file_name), 'wb') as f:
            while True:
                data = incoming_sock.recv(1024)
                if not data: break
                f.write(data)


    def compress(self, i, o):
        out = tarfile.open(o, 'w:gz')
        for infile in i:
            out.add(infile, arcname=os.path.split(infile)[-1])
        out.close()

    def check_hosts(self):
        alive = False
        if self.mode == 'gui':
            hosts =self.hosts.get(0, END)
            if hosts:
                bad_hosts = []
                for host in hosts:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    host, port = host.split(':')
                    port = int(port, base=10)
                    try:
                        sock.connect((host, port))
                        sock.send(str(self.CHECK_ALIVE))
                        data = sock.recv(1024)
                        if not int(data) == self.ALIVE:
                            bad_hosts.append('%s:%s' % (host,port))
                    except socket.error:
                        bad_hosts.append(host)
                if bad_hosts:
                    tkMessageBox.showwarning('Connection error', 'Failed to connect to \n%s.' % '\n'.join(bad_hosts))
                else:
                    alive = True
            else:
                tkMessageBox.showwarning('No hosts specified', 'Please specify the hosts you want to connect to.')
        elif self.mode == 'cmd_line':
            hosts =self.hosts
            if hosts:
                bad_hosts = []
                for host in hosts:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    host, port = host.split(':')
                    port = int(port, base=10)
                    try:
                        sock.connect((host, port))
                        sock.send(str(self.CHECK_ALIVE))
                        data = sock.recv(1024)
                        if not int(data) == self.ALIVE:
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
        if self.mode == 'gui':
            files = self.files.get(0, END)
            if files:
                bad_files = []
                for f in files:
                    if not os.path.exists(f):
                        bad_files.append(f)
                if bad_files:
                    tkMessageBox.showwarning('Files not found', 'File \n%s\ncan not be found.' % '\n'.join(bad_files))
                    return False
                else:
                    return True
            else:
                tkMessageBox.showwarning('No files specified', 'Please specify the files that you want to distribute.')
                return False
        elif self.mode == 'cmd_line':
            files = self.files
            if files:
                bad_files = []
                for f in files:
                    if not os.path.exists(f):
                        bad_files.append(f)
                if bad_files:
                    print  'File \n%s\ncan not be found.' % '\n'.join(bad_files)
                    return False
                else:
                    return True
            else:
                print 'Please specify the files that you want to distribute.'
                return False


    def check_template(self):
        if self.mode == 'gui':
            template = self.template.get()
            if template:
                if os.path.exists(template):
                    return True
                else:
                    tkMessageBox.showwarning('File not exists', 'File "%s" does not exist.' % (template,))
                    return False
            else:
                tkMessageBox.showwarning('No template file specified', 'Please specify the template file for your work.')
                return False
        elif self.mode == 'cmd_line':
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

    def pack_files(self):
        if self.mode == 'gui':
            n_sys = len(self.hosts.get(0, END))
            template_name = self.template.get()
        elif self.mode == 'cmd_line':
            n_sys = len(self.hosts)
            template_name = self.template

        if self.rename:
            n_files = len(os.listdir('to_be_distributed'))
        else:
            filenames = os.listdir('to_be_distributed')
            pattern = '[0-9]+\.'
            filenames.sort(key = lambda x: int(re.findall(pattern, x)[-1][:-1]))

            n_files = int(re.findall(pattern, filenames[-1])[-1][:-1], base=10)

        if (n_files % n_sys):
            sys_size = n_files / n_sys + 1
            last = sys_size + n_files%n_sys - n_sys
        else:
            sys_size = n_files / n_sys
            last = sys_size

        i = 1
        for n in xrange(1, n_sys+1):
            if not self.rename:
                if n == n_sys:
                    pattern = '|'.join( "\D0*%d\.\w+$|^0*%d\.\w+$" % (x,x) for x in xrange(n, n_files+1, n_sys))
                else:
                    pattern = '|'.join( "\D0*%d\.\w+$|^0*%d\.\w+$" % (x,x) for x in xrange(n, n_files+1, n_sys))
                sub_filenames = filter(lambda x: re.search(pattern, x), filenames)
            else:
                if n == n_sys:
                    sub_filenames = map(str, range(i, i+last))
                else:
                    sub_filenames = map(str, range(i, i+sys_size))
            i += sys_size


        # generate script
            with open(template_name, 'r') as template:
                template_script = template.readlines()
            with open('to_be_distributed/%d.sh' % (n,), 'w') as f:
                cd = 'cd $1\n'
                files = 'files="' + ' '.join(sub_filenames) + '"\n'
                f.write(cd)
                f.write(files)
                for line in template_script:
                    f.write(line)
        # pack files
            ins = sub_filenames + ['%d.sh' % (n,)]
            ins = [os.path.join('to_be_distributed', x) for x in ins]
            out = os.path.join('to_be_distributed', 'sys-%d.tar.gz' % (n,))
            self.compress(ins, out)


    def copy_files(self):
        if self.mode == 'gui':
            old_file_names = self.files.get(0, END)
        elif self.mode == 'cmd_line':
            old_file_names = self.files
        if self.rename:
            new_file_names = map(str, range(1, len(old_file_names)+1))
        else:
            new_file_names = map(lambda x: os.path.split(x)[-1], old_file_names)

        if os.path.exists('to_be_distributed'):
            shutil.rmtree('to_be_distributed')
        os.mkdir('to_be_distributed')
        new_file_names = map(lambda x: os.path.join('to_be_distributed', x), new_file_names)

        for src, dst in itertools.izip(old_file_names, new_file_names):
            shutil.copyfile(src, dst)


    def quit(self):
        self.parent.destroy()

    def configure(self, hosts, files, template, rename):
        """ Since the 'configure' method is separate from the '__init__' method, 'configure'
        must be called explictly after an instance is created in order for it to run properly.
        """
        self.rename = rename
        if self.mode == 'gui':
            if hosts:
                hosts = [ x if re.search(r'^.+\:[1-9]\d*$', x) else '%s:%s' % (x, self.client_listen_port) for x in hosts ]
                self.hosts.insert(0, *hosts)
            if files:
                self.files.insert(0, *files)
            if template:
                self.template.insert(0, template)
        elif self.mode == 'cmd_line':
            if hosts:
                self.hosts = [ x if re.search(r'^.+\:[1-9]\d*$', x) else '%s:%s' % (x, self.client_listen_port) for x in hosts ]
                print self.hosts
            self.files = files
            self.template = template


def main():
    mode = 'gui'
    direct, rename = False, False
    hosts, files, template = ['']*3

    opts, args = getopt.getopt(sys.argv[1:], 'f:F:cdh:H:t:r')
    for opt, arg in opts:
        if opt == '-c':
            mode = 'cmd_line'
        elif opt == '-d':
            direct = True
        elif opt == '-f':
            files = arg.split()
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
        elif opt == '-r':
            rename = True
    if mode == 'gui':
        root = Tk()
        distributor = Distributor(root)
        distributor.configure(hosts, files, template, rename)
        if direct:
            myapp.start()
        root.mainloop()
    elif mode == 'cmd_line':
        distributor = Distributor(mode='cmd_line')
        distributor.configure(hosts, files, template, rename)
        distributor.start()
        for mutex in myapp.exit_mutexes:
            while not mutex.locked(): pass
        print 'Program terminated.'

if __name__=="__main__":main()

