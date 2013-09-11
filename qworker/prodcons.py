'''
Created on May 24, 2012

@author: ray
'''
import time
import logging
import signal
import Queue
import traceback
from multiprocessing import Process, JoinableQueue, Event, cpu_count

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

BLOCK_TIMEOUT = 0.01


__all__ = ['Producer', 'Consumer', 'Mothership']


class TerminalException(KeyboardInterrupt):
    pass

#==============================================================================
# Producer Proxy
#==============================================================================
class ProducerProxy(Process):

    """ Producer Proxy

    Producer Proxy act as a broker for producer in Producer-Consumer Pattern.
    It deals with the process model for the real producer.
    """

    def __init__(self, queue, producer):
        Process.__init__(self)
        self._queue = queue
        self._producer = producer
        self._stop = Event()
        self._stop.clear()

    def items(self):
        return self._producer.items()

    def close(self):
        self._producer.close()

    def stop(self):
        self._stop.set()

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        for item in self.items():

            if self._stop.is_set():
                self.close()
                break

            while True:
                try:
                    self._queue.put(item, timeout=BLOCK_TIMEOUT)
                except Queue.Full:
                    continue
                else:
                    break


#==============================================================================
# Consumer Proxy
#==============================================================================
class ConsumerProxy(Process):

    """ Consumer Proxy

    Consumer Proxy act as a broker for consumer in Producer-Consumer Pattern.
    It deals with the process model for the real consumer.
    """

    def __init__(self, queue, consumer):
        Process.__init__(self)
        self._queue = queue
        self._consumer = consumer
        self._stop = Event()
        self._stop.clear()

    def consume(self, task):
        self._consumer.consume(task)

    def close(self):
        self._consumer.close()

    def stop(self):
        self._stop.set()

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        while True:

            if self._stop.is_set():
                self.close()
                break

            try:
                task = self._queue.get(timeout=BLOCK_TIMEOUT)
            except Queue.Empty:
                continue

            try:
                self.consume(task)
            except Exception:
                traceback.print_exc()
            finally:
                self._queue.task_done()


#==============================================================================
# Master and Slaver
#==============================================================================
class Producer(object):

    """ Producer Interface """

    def items(self):
        """ Returns a iterable of task object """
        yield
        return


class Consumer(object):

    """ Consumer Interface """

    def consume(self, task):
        """ Deals with the task """
        raise NotImplementedError


#==============================================================================
# Monitor
#==============================================================================
class Mothership(object):

    """ Monitor of producer and consumers """

    def __init__(self, producer, consumers, graceful=False):
        self._queue = JoinableQueue()

        self._producer_proxy = ProducerProxy(self._queue, producer)
        self._consumer_pool = list(ConsumerProxy(self._queue, cons) for cons in consumers)
        self._graceful = graceful

    def start(self):
        try:
            """ Start working """
            logger.info('Starting Producers'.center(20, '='))
            self._producer_proxy.start()

            time.sleep(0.1)

            logger.info('Starting Consumers'.center(20, '='))
            for consumer in self._consumer_pool:
                consumer.start()

            self._producer_proxy.join()
            self._queue.join()
            for consumer in self._consumer_pool:
                consumer.join()

            self._queue.close()

        except KeyboardInterrupt:

            self._producer_proxy.stop()
            self._producer_proxy.join()

            if self._graceful:
                logger.info('Shutting Down gracefully...')
                self._queue.join()

            for consumer in self._consumer_pool:
                consumer.stop()
                consumer.join()

            self._queue.close()

    def __enter__(self):
        return self

    def __exit__(self, types, value, tb):
        return


#==============================================================================
# Test
#==============================================================================
class TestMaster(Producer):

    def items(self):
        for i in range(100):
            yield i

    def close(self):
        logger.info('Master stopped')


class TestSlaver(Consumer):

    def __init__(self, tag):
        self._tag = tag

    def consume(self, task):
        logger.info('%s-%d' % (self._tag, task))
        time.sleep(0.1)

    def close(self):
        logger.info('%s stopped' % self._tag)


def main():

    master = TestMaster()
    slavers = (TestSlaver('slaver-%d' % i) for i in range(cpu_count() + 4))

    with Mothership(master, slavers) as m:
        m.start()

if __name__ == "__main__":
    main()
