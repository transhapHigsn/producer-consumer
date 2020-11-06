import logging
# import resource

from queue import Queue
from random import randrange
from threading import Thread
from time import sleep


def add(a, b):
    return a + b


class QueuePool:
    def __init__(self):
        self.pool = {}

    def get_queue(self, tag):
        queue = self.pool.get(tag)
        if not queue:
            queue = Queue()
            self.pool[tag] = queue

        return queue

    def get(self, tag):
        return self.get_queue(tag=tag).get()

    def task_done(self, tag):
        return self.get_queue(tag=tag).task_done()

    def put(self, tag, target, *args, **kwargs):
        return self.get_queue(tag=tag).put({
            "target": target,
            "args": args,
            "kwargs": kwargs,
            "tag": tag
        })


class Consumer(Thread):
    def __init__(self, queue_pool, name, tag):
        Thread.__init__(self, name=name)
        self.job_queue = queue_pool.get_queue(tag=tag)
        self.tag = tag

    def run(self):
        while True:
            job_info = self.job_queue.get()
            try:
                resp = job_info["target"](*job_info["args"], **job_info["kwargs"])
                logger.info(f"Name: {self.name} Args: {job_info['args']} Kwargs: {job_info['kwargs']} Response: {resp}") 
            finally:
                self.job_queue.task_done()


class Producer(Thread):
    def __init__(self, queue_pool, name, tag):
        Thread.__init__(self, name=name)
        self.queue_pool = queue_pool
        self.tag = tag
        self.wait = randrange(2, 5, 1)

    def run(self):
        i = 0
        while True:
            logger.info(f"Name: {self.name} Tag: {self.tag} Wait: {self.wait}")
            self.queue_pool.put(
                "con",
                add,
                *(),
                **{"a": i, "b": i+3},
            )
            i += 1
            sleep(self.wait)


class ThreadPool:
    def __init__(self, max_consumer_pool_size, max_producer_pool_size):
        self.max_consumer_pool_size = max_consumer_pool_size
        self.max_producer_pool_size = max_producer_pool_size

        self.current_consumer_pool_size = 0
        self.current_producer_pool_size = 0

    def add(self, queue_pool, tag, _type):
        if _type == "consumer":
            if self.current_consumer_pool_size == self.max_consumer_pool_size:
                raise Exception("MaxConsumerPoolSizeReached")

            thread_name = f"{tag}-{self.current_consumer_pool_size}"

            thread = Consumer(queue_pool=queue_pool, name=thread_name, tag=tag)
            self.current_consumer_pool_size += 1
        else:
            if self.current_producer_pool_size == self.max_producer_pool_size:
                raise Exception("MaxConsumerPoolSizeReached")

            thread_name = f"{tag}-{self.current_producer_pool_size}"

            thread = Producer(queue_pool=queue_pool, name=thread_name, tag=tag)
            self.current_producer_pool_size += 1

        thread.start()


    def add_job(self, queue_pool, tag, target, *args, **kwargs):
        return queue_pool.put(
            tag,
            target,
            *args,
            **kwargs,
        )


def start_workers(pool, queue_pool, number_of_producers, number_of_consumers):
    try:
        for _ in range(number_of_producers):
            pool.add(queue_pool=queue_pool, _type="producer", tag="pro")

        for _ in range(number_of_consumers):
            pool.add(queue_pool=queue_pool, _type="consumer", tag="con")
    except Exception as e:
        logging.error(f"Program stopped: {e}")


def create():
    queue_pool = QueuePool()
    pool = ThreadPool(max_consumer_pool_size=10, max_producer_pool_size=10)

    # for i in range(1000):
    #     pool.add_job(queue_pool=queue_pool, tag="con", target=add, a=i, b=i+2)

    start_workers(pool=pool, queue_pool=queue_pool, number_of_consumers=10, number_of_producers=2)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # cannot figure out memory usage part :disappointed:
    # soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    # max_memory_usage = 2 * 1024 * 1024
    # resource.setrlimit(resource.RLIMIT_AS, (max_memory_usage, max_memory_usage))
    create()
