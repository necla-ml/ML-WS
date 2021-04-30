import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import wraps

TASKS = {}
task = lambda f: TASKS.setdefault(f.__name__, f)

def timeit(f):
    """
    Time function executions
    Params: 
        f: function to be timed
    """
    def timed(*args, **kw):
        ts = time.time()
        result = f(*args, **kw)
        te = time.time()
        print(f'Function: {f.__name__}')
        # print(f'*  args: {args}')
        # print(f'*  kw: {kw}')
        print(f'*  execution time: {(te-ts)*1000:8.2f} ms')
        return result
    return timed

def retry(exceptions, tries=4, delay=3, backoff=2, max_delay=24, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Params:
        exceptions: The exception to check. may be a tuple of exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay each retry).
        max_delay: Maximum delay before reset to original delay
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1 or mtries < 0:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    if e:
                        msg = f'Exception: {e}, Retrying in {mdelay} seconds...'
                    else:
                        msg = f'Retrying in {mdelay} seconds...'

                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    if mdelay >= max_delay:
                        mdelay = delay
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

def sizeof(obj, seen=None):
    """Recursively and fully calculate the size of an object"""

    import sys
    from collections.abc import Mapping, Iterable
    obj_id = id(obj)
    try:
        if obj_id in seen:
            return 0
    except TypeError:
        seen = set()

    seen.add(obj_id)

    size = sys.getsizeof(obj)

    # since strings are iterabes we return their size explicitly first
    if isinstance(obj, str):
        return size
    elif isinstance(obj, Mapping):
        return size + sum(
            sizeof(key, seen) + sizeof(val, seen)
            for key, val in obj.items()
        )
    elif isinstance(obj, Iterable):
        return size + sum(
            sizeof(item, seen)
            for item in obj
        )

    return size

class Signaled():
    def __init__(self):
        self._signal = False

    @property
    def signal(self):
        return self._signal

    @signal.setter
    def signal(self, value):
        self._signal = value

    def is_set(self):
        return self._signal == True

class ParallelExecutor:
    """
    Run list of tasks in parallel
    """
    def __init__(self, cpu_bound=False, max_workers=4):
        self._executor = cpu_bound and ProcessPoolExecutor(max_workers=max_workers) or ThreadPoolExecutor(max_workers=max_workers)
        self.tasks = []

    def run(self, tasks, timeout=None):
        """
        Params:
            tasks: list of tasks (func, arg1, arg2, arg3)
            timeout: maximum number of seconds to wait before returning
        """
        loop = asyncio.get_event_loop()

        executor = self._executor

        results = []
        self.tasks = tasks = [loop.run_in_executor(executor, *args) for args in tasks]
        done, pending = loop.run_until_complete(asyncio.wait(tasks, timeout=timeout, return_when=asyncio.ALL_COMPLETED))
        for task in tasks:
            try:
                res = task.result()
            except Exception as e:
                res = e
            results.append(res)

        return results

    def close(self):
        for task in self.tasks:
            task.cancel()
        self._executor.shutdown(wait=True)
    
    def __iter__(self):
        return iter(self.tasks)



