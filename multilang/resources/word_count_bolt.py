import storm
# Counter is a nice way to count things,
# but it is a Python 2.7 thing
from collections import Counter


class CountBolt(storm.BasicBolt):
    
    word_cnt = dict()
    
    # Initialize this instance
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        
        self.word_cnt = dict()

        storm.logInfo("Counter bolt instance starting...")

        # Hint: Add necessary instance variables and classes if needed

    def process(self, tup):
        # Get the word from the inbound tuple
        word = tup.values[0]
        
        if word not in self.word_cnt:
            self.word_cnt[word] = 0
        # Increment the counter
        self.word_cnt[word] += 1
        
        count = self.word_cnt[word]
        storm.logInfo("Emitting %s:%s" % (word, count))
        # Emit the word and count
        storm.emit([{'word': word, 'count': count}])
        # TODO
        # Task: word count
        # Hint: using instance variable to tracking the word count
        pass
        # End


# Start the bolt when it's invoked
CountBolt().run()
