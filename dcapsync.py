#!/usr/bin/env python
# -*- coding: utf-8 -*-

from threading import Thread
import logging
import time
from Queue import Queue
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from dcap import Dcap


class Sceduler:

  def __init__(self, config):

    fs = config.get('scheduler', 'fs', 0)
    dest = config.get('store', 'path', 0)
    self.ioqueue = Queue()
    self.iothread = Thread(target=self.ioprocess)
    self.iothread.daemon = True
    self.observer = Observer()
    self.event_handler = IoTask(self.ioqueue, fs, dest)
    self.observer.schedule(self.event_handler, fs, recursive=True)

  def ioprocess(self):
    while True:
      t = self.ioqueue.get()
      try:
        t.process()
      finally:
        self.ioqueue.task_done()

  def start(self):
    self.observer.start()
    self.iothread.start()

  def stop(self):
    self.observer.stop()
    self.iothread.stop()

  def join(self):
     self.observer.join()
     self.iothread.join()

class IoTask(FileSystemEventHandler):

  def __init__(self, queue, fs, dest):
    self.queue = queue
    self.fs = fs
    self.dest = dest

  def on_moved(self, event):

        what = 'directory' if event.is_directory else 'file'
        logging.info("Moved %s: from %s to %s", what, event.src_path, event.dest_path)
        #iotask = IoTaskCreate(self.fs, event.src_path, self.dest, event.is_directory)
        #self.queue.put(iotask)

  def on_created(self, event):

        what = 'directory' if event.is_directory else 'file'
        logging.info("Created %s: %s", what, event.src_path)
        iotask = DCapTaskCreate(self.fs, event.src_path, self.dest, event.is_directory)
        self.queue.put(iotask)

  def on_deleted(self, event):

        what = 'directory' if event.is_directory else 'file'
        logging.info("Deleted %s: %s", what, event.src_path)
        #iotask = IoTaskDelete(self.fs, event.src_path, self.dest, event.is_directory)
        #self.queue.put(iotask)

  def on_modified(self, event):

        what = 'directory' if event.is_directory else 'file'
        logging.info("Modified %s: %s", what, event.src_path)

	iotask = DCapTaskCreate(self.fs, event.src_path, self.dest, event.is_directory)
        self.queue.put(iotask)

from shutil import *
import string
import os

class IoTasklet:

  def __init__(self, fs, path, dest, is_directory):    
    self.dest = os.path.join(dest, os.path.relpath(path, fs))
    self.fs = fs
    self.path = path
    self.is_directory = is_directory

  def process(self):
    pass

class IoTaskCreate(IoTasklet):
  def __init__(self, fs, path, dest, is_directory):
    IoTasklet.__init__(self, fs, path, dest, is_directory)

  def process(self):
    if self.is_directory:
      os.mkdir(self.dest)
    else:
      copy2(self.path, self.dest)

class IoTaskDelete(IoTasklet):
  def __init__(self, fs, path, dest, is_directory):
    IoTasklet.__init__(self, fs, path, dest, is_directory)

  def process(self):
    if self.is_directory:
      os.rmdir(self.dest)
    else:
      os.remove(self.dest)

class IoTaskModify(IoTasklet):
  def __init__(self, fs, path, dest, is_directory):
    IoTasklet.__init__(self, fs, path, dest, is_directory)

  def process(self):
    if self.is_directory:
      pass
    else:
      copy2(self.path, self.dest)


GENERATION_FORMAT = '%s_PB%d$'

class DCapTaskCreate(IoTasklet):
  def __init__(self, fs, path, dest, is_directory):
    IoTasklet.__init__(self, fs, path, dest, is_directory)
    self.dcap = Dcap("dcap://dcache-lab000.desy.de:22125")

  def process(self):
    if not self.is_directory:
      version = GENERATION_FORMAT % (self.dest, time.time())
      self.dcap.rename(self.dest, version)
      f = self.dcap.open_file(self.dest, 'w')
      f.send_file(self.path)
      f.close()
  
  def __del__(self):
    self.dcap._send_bye()
    self.dcap._close()

if __name__ == '__main__':
  import ConfigParser, os

  logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

  config = ConfigParser.ConfigParser()
  config.read('dcapsync.conf')

  s = Sceduler(config)
  s.start()

  try:
     while True:
       time.sleep(1)
  except KeyboardInterrupt:
    s.stop()
  s.join()
