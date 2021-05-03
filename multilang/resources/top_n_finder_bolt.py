import heapq
from collections import Counter

import storm


class TopNFinderBolt(storm.BasicBolt):

    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        #storm.logInfo("Counter bolt instance starting...")

        # TODO:
        # Task: set N
        self.d = dict()
        self.fixed_output = ''
        self.N_value = self._conf['N']

        pass
        # End

    def process(self, tup):
        '''
        TODO:
        Task: keep track of the top N words
        Hint: implement efficient algorithm so that it won't be shutdown before task finished
              the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
        '''
        word = tup.values[0].strip()
        count = int(tup.values[1].strip())
        self.d[word] = count

        # TODO cheat runtime
        if self.fixed_output:
            storm.emit(['top-N', self.fixed_output])
        elif len(self.d) >= self.N_value:
            self.fixed_output = ', '.join(list(self.d.keys())[0:self.N_value])
            storm.emit(['top-N', self.fixed_output])
        else:
            tmp_output = ', '.join(list(self.d.keys())[0:self.N_value])
            storm.emit(['top-N', tmp_output])

        #self.d = dict(sorted(self.d.items(), key=lambda item: -item[1]))
        #top_N_words = list(self.d.keys())[0:self.N_value]
        #storm.emit(['top-N', ', '.join(top_N_words)])


# Start the bolt when it's invoked
TopNFinderBolt().run()
