import sys
import time
from itertools import product
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

from newio import run_in_thread, spawn, run
from newio.channel import Channel


_QUEUE_CLOSE = object()

NUM_ITEMS = 10 ** 3


def queue_producer(queue):
    for i in range(NUM_ITEMS):
        queue.put(1)


def queue_consumer(queue):
    total = 0
    while True:
        i = queue.get()
        if i is _QUEUE_CLOSE:
            queue.put(_QUEUE_CLOSE)
            break
        total += i
    return total


async def thread_producer(channel):
    channel = channel.sync

    def _producer():
        for i in range(NUM_ITEMS):
            channel.send(1)

    return await run_in_thread(_producer)


async def thread_consumer(channel):
    channel = channel.sync

    def _consumer():
        total = 0
        for i in channel:
            total += i
        return total

    return await run_in_thread(_consumer)


async def newio_producer(channel):
    for i in range(NUM_ITEMS):
        await channel.send(1)


async def newio_consumer(channel):
    total = 0
    async for i in channel:
        total += i
    return total


def benchmark_queue(num_producer, num_consumer):
    t0 = time.monotonic()
    queue = Queue(128)
    executor = ThreadPoolExecutor(num_producer + num_consumer)
    producers = []
    consumers = []
    for _ in range(num_producer):
        producers.append(executor.submit(queue_producer, queue))
    for _ in range(num_consumer):
        consumers.append(executor.submit(queue_consumer, queue))
    t1 = time.monotonic()
    for fut in producers:
        fut.result()
    queue.put(_QUEUE_CLOSE)
    t2 = time.monotonic()
    total = 0
    for fut in consumers:
        total += fut.result()
    assert total == len(producers) * NUM_ITEMS
    executor.shutdown()
    t3 = time.monotonic()
    return [total, t1 - t0, t2 - t1, t3 - t2]


async def benchmark_channel(producer, num_producer, consumer, num_consumer):
    t0 = time.monotonic()
    producers = []
    consumers = []
    async with Channel() as channel:
        for _ in range(num_producer):
            producers.append(await spawn(producer(channel)))
        for _ in range(num_consumer):
            consumers.append(await spawn(consumer(channel)))
        t1 = time.monotonic()
        for task in producers:
            await task.join()
        await channel.close()
        t2 = time.monotonic()
        total = 0
        for task in consumers:
            total += await task.join()
    assert total == len(producers) * NUM_ITEMS
    t3 = time.monotonic()
    return [total, t1 - t0, t2 - t1, t3 - t2]


producers = [thread_producer, newio_producer]
consumers = [thread_consumer, newio_consumer]


def sout(text):
    sys.stdout.write(text)
    sys.stdout.flush()


class Printer:
    def __init__(self):
        self.titles = []
        self.results = []

    def format_title(self, p, pn, c, cn):
        title = f'{p:>15} {pn:>1d} : {cn:>1d} {c:>15}'
        return f'{title:>40}'

    def print_title(self, p, pn, c, cn):
        self.titles.append((p, pn, c, cn))
        sout(self.format_title(p, pn, c, cn))

    def format_result(self, result):
        total = result[0]
        result = result[1:]
        t_total = sum(result)
        qps = '{:>7d}'.format(int(total / t_total))
        t_setup, t_producer, t_consumer = [f'{x:.3f}' for x in result]
        t = f'{t_setup} + {t_producer} + {t_consumer}'
        return f' => {t} = {t_total:.3f}  {qps} QPS'

    def print_result(self, result):
        self.results.append(result)
        sout(self.format_result(result) + '\n')

    def _sort_key(self, title_result):
        _, result = title_result
        total = result[0]
        t_total = sum(result[1:])
        return total / t_total

    def print_sorted(self):
        print('-' * 80)
        items = sorted(zip(self.titles, self.results), key=self._sort_key, reverse=True)
        for title, result in items:
            sout(self.format_title(*title))
            sout(self.format_result(result) + '\n')


def benchmark(p_name='', p_n=(1, 9), c_name='', c_n=(1, 9)):
    printer = Printer()
    for num_producer, num_consumer in product(p_n, c_n):
        if p_name not in 'queue_producer' or c_name not in 'queue_consumer':
            continue
        printer.print_title(
            'queue_producer', num_producer, 'queue_consumer', num_consumer
        )
        result = benchmark_queue(num_producer, num_consumer)
        printer.print_result(result)
    for producer, num_producer, consumer, num_consumer in product(
        producers, p_n, consumers, c_n
    ):
        if p_name not in producer.__name__ or c_name not in consumer.__name__:
            continue
        printer.print_title(
            producer.__name__, num_producer, consumer.__name__, num_consumer
        )
        result = run(benchmark_channel(producer, num_producer, consumer, num_consumer))
        printer.print_result(result)
    printer.print_sorted()
