#!/mnt/orange/brew/data/bin/python3
import os
from util import load_config

cfg = load_config()

activator = cfg['activator']
with open(activator) as f:
    exec(f.read(), {"__file__": activator})

from wsgiref.handlers import CGIHandler
from app import app
CGIHandler().run(app)
