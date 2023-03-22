#!/usr/bin/env python3

from contract import *
import _thread


pContract=PrivacyContract()
_thread.start_new_thread(pContract.runJobsLoop, (2,))
pContract.eventMonitor()
