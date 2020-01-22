import json
import os
class Config(object):
    def __init__(self,):
        self.configfile = os.path.join(os.path.dirname(os.path.dirname(__file__)),"config.json")
        if not os.path.exists(self.configfile):
            self.config=""
        else:
            with open(self.configfile,'r') as f:
                self.config=json.load(f)

config=Config().config