import heapq
from collections import Counter

import storm
import heapq


"""
top_cands = []
heapq.heapify(top_cands)
heapq.heappush(top_cands, Node('Matta', 2))
heapq.heappush(top_cands, Node('Long', 10))
heapq.heappush(top_cands, Node('Tin', 3))
############
heapq.heapify(top_cands)
heapq.heappush(top_cands, Node('Van', 20))
heapq.heapreplace(top_cands, Node('Tran', 100))
print(heapq.heappop(top_cands))
print(heapq.heappop(top_cands))
print(heapq.heappop(top_cands))
print(heapq.heappop(top_cands))
"""


class TopNFinderBolt(storm.BasicBolt):

    class Node:
        def __init__(self, word, cnt_map):
            self.word = word
            self.cnt_map = cnt_map

        def __repr__(self):
            # return '({}, {})'.format(self.word, self.get_count())
            return self.word

        def __lt__(self, other):
            return self.get_count() < other.get_count()

        def get_count(self):
            return self.cnt_map[self.word]

    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context

        #storm.logInfo("Counter bolt instance starting...")

        # Task: set N
        self.N_value = int(self._conf['N'])

        # use a min heap
        self.top_cands = []
        heapq.heapify(self.top_cands)
        # map word -> latest count
        self.cnt_map = dict()

    def process(self, tup):
        '''
        TODO:
        Task: keep track of the top N words
        Hint: implement efficient algorithm so that it won't be shutdown before task finished
              the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
        '''
        word = tup.values[0].strip()
        count = int(tup.values[1].strip())

        # changing within the top group is a no-op, just a count update
        if word in self.cnt_map:
            self.cnt_map[word] = count
            # heapify because of count update
            heapq.heapify(self.top_cands)
            #self.__do_emit_updated(is_updated=False)
        else:
            # if the list still growing, it's also a no-op, just a new word/count ADD
            if len(self.top_cands) < self.N_value:
                self.cnt_map[word] = count
                heapq.heappush(self.top_cands, TopNFinderBolt.Node(word, self.cnt_map))
                self.__do_emit_updated(is_updated=True)
            # decide whether to replace the weakest candidate in self.top_cands
            else:
                if self.top_cands[0].get_count() < count:
                    self.cnt_map[word] = count
                    exist_min_word = self.top_cands[0].word
                    heapq.heapreplace(self.top_cands, TopNFinderBolt.Node(word, self.cnt_map))
                    del self.cnt_map[exist_min_word]
                    #heapq.heapify(self.top_cands)
                    self.__do_emit_updated(is_updated=True)
                else:
                    #self.__do_emit_updated(is_updated=False)
                    pass

        """
        # TODO cheat runtime
        if self.last_output:
            storm.emit(['top-N', self.last_output])
        elif len(self.the_top) >= self.N_value:
            self.last_output = ', '.join(list(self.the_top.keys())[0:self.N_value])
            storm.emit(['top-N', self.last_output])
        else:
            tmp_output = ', '.join(list(self.the_top.keys())[0:self.N_value])
            storm.emit(['top-N', tmp_output])
        """

        #self.d = dict(sorted(self.d.items(), key=lambda item: -item[1]))
        #top_N_words = list(self.d.keys())[0:self.N_value]
        #storm.emit(['top-N', ', '.join(top_N_words)])

    def __do_emit_updated(self, is_updated=True):
        #if not self.last_output or is_updated:
        #    self.last_output = ', '.join(list(self.the_top.keys()))
        #storm.emit(['top-N', self.last_output])
        #output = ', '.join(list(self.the_top.keys()))
        output = ', '.join([cand.word for cand in self.top_cands])
        storm.emit(['top-N', output])


# Start the bolt when it's invoked
# TODO enable
TopNFinderBolt().run()
