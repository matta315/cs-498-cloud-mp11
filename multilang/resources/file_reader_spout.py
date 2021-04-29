# import os
# from os.path import join
from time import sleep

# from streamparse import Spout
import storm


class FileReaderSpout(storm.Spout):

    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self._complete = False

        #storm.logInfo("Spout instance starting...")

        # TODO:
        # Task: Initialize the file reader
        self.ff = open(self._conf['input_file'], "r")
        # End

    def nextTuple(self):
        # TODO:
        #sleep(0.1)
        # Task 1: read the next line and emit a tuple for it
        line = self.ff.readline()
        if line is not None:
            if line.strip() != '':
                #storm.logInfo("Emiting %s" % line)
                storm.emit([line.strip()])
        else:
            self.ff.close()
            sleep(1)


# Start the spout when it's invoked
FileReaderSpout().run()
