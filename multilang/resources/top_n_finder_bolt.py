import heapq
from collections import Counter

import storm


class TopNFinderBolt(storm.BasicBolt):
    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        storm.logInfo("Counter bolt instance starting...")

        # TODO:
        # Task: set N
        self.d = dict()

        pass
        # End

        # Hint: Add necessary instance variables and classes if needed

    def process(self, tup):
        '''
        TODO:
        Task: keep track of the top N words
        Hint: implement efficient algorithm so that it won't be shutdown before task finished
              the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
        '''
        word = tup.values[0].strip()
        count = tup.values[1].strip()
        self.d[word] = count

        self.d = dict(sorted(self.d.items(), key=lambda item: item[1]))
        top_10_words = list(self.d.keys())[0:10]

        storm.emit(['top-N', ', '.join(top_10_words)])
        # End


# Start the bolt when it's invoked
TopNFinderBolt().run()
