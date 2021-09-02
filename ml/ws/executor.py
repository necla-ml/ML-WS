from abc import ABC, abstractmethod

class Executor(ABC):
    """Base executor class for multi threaded modules with start and stop control
    """
    def __init__(self, name=None):
        super(Executor, self).__init__()
        self.name = name or self.__class__.__name__
        self._running = 0
        self._runner = None
        self.stop_event = None
        self.exception_count = 0

    @property
    def running(self):
        return self._running

    @running.setter
    def running(self, v):
        self._running = (1 if v else 0)

    @abstractmethod
    def run(self):
        """ Child classes need to implement this method"""
        pass
        
    def start(self):
        if self.running:
            print(f'{self.name}: already started')
            return

        from threading import Thread, Event

        self.running = True
        self.stop_event = Event()
        self._runner = Thread(name=self.name, target=self.run, args=(), kwargs={})
        self._runner.daemon = True 
        self._runner.start()

    def stop(self, block=True, timeout=None):
        self.running = False
        # reset exception count
        self.exception_count = 0
        self.stop_event.set()
        if block:
            self.join(timeout=timeout)

    def join(self, timeout=None):
        self._runner.join(timeout=timeout)
        self._runner = None
