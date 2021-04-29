import storm
import re


class SplitBolt(storm.BasicBolt):
    # There's nothing to initialize here,
    # since this is just a split and emit
    # Initialize this instance

    # Contruct regular expression to split word later
    # with open(delimitersPath) as f:
    #     delimiters = f.read().strip()
    # sepr_list = [" "]
    # for c in delimiters:
    #     sepr_list.append(c)
    # reg_exp = '|'.join(map(re.escape, sepr_list))



    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        #storm.logInfo("Split bolt instance starting...")

    def process(self, tup):
        # Split the inbound sentence at spaces
        reg = '[^a-zA-Z0-9-]'
        line = tup.values[0].strip()
        words = re.split(reg, line)
        # Loop over words and emit
        for word in words:
            #storm.logInfo("Emitting %s" % word)
            storm.emit([word])

        # TODO
        # Task: split sentence and emit words
        # Hint: split on "[^a-zA-Z0-9-]"
        pass
        # End


# Start the bolt when it's invoked
SplitBolt().run()
