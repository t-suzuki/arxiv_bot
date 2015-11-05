#!env python
# -*- coding: utf-8 -*-
# throttle function call.
import time
import logging

def throttle(delay_s):
    u'''throttled function call'''
    def _wrapper(f):
        def _f(*va, **kwa):
            funcs = getattr(throttle, 'funcs', {})
            if funcs.has_key(f.__name__):
                s = funcs[f.__name__] - time.time()
                if s > 0:
                    msg = 'throttling.. delay: {:.2}'.format(s)
                    log = logging.getLogger()
                    if log is None:
                        print(msg)
                    else:
                        log.debug(msg)
                    time.sleep(s)
            res = f(*va, **kwa)
            funcs[f.__name__] = time.time() + delay_s
            setattr(throttle, 'funcs', funcs)
            return res
        return _f
    return _wrapper

