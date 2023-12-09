# -*- coding: utf-8 -*-
"""Operator support

"""
# import cPickle
import cProfile
# noinspection PyCompatibility,PyPep8Naming
# import cPickle as pickle
import pickle
import multiprocessing
# import threading
import time
import traceback
import pandas as pd
import math

from s3filter.multiprocessing.channel import Channel
from s3filter.multiprocessing.handler_base import HandlerBase
from s3filter.multiprocessing.message import StartMessage, StopMessage, DataFrameMessage, MessageBase
from s3filter.multiprocessing.packet import PacketBase

from s3filter.multiprocessing.worker import Worker
from s3filter.multiprocessing.worker_system import WorkerSystem


def switch_context(from_op, to_op):
    """Handles a context switch from one operator to another. This is used to stop the sending operators
    timer and start the receiving operators timer.

    :param from_op: Operator switching context from
    :param to_op: Operator switching context to
    :return: None
    """

    if not from_op.op_metrics.timer_running():
        raise Exception("Illegal context switch. "
                        "Attempted to switch from operator '{}' with stopped timer"
                        .format(from_op.name))

    if to_op.op_metrics.timer_running():
        raise Exception("Illegal context switch. "
                        "Attempted to switch to operator '{}' with running timer"
                        .format(from_op.name))

    from_op.op_metrics.timer_stop()
    to_op.op_metrics.timer_start()


# class StartMessage(object):
#     pass
#
#
# class StopMessage(object):
#     pass


class ProducerCompletedMessage(MessageBase):

    def __init__(self, producer_name):
        super(ProducerCompletedMessage, self).__init__()
        self.producer_name = producer_name


class ConsumerCompletedMessage(MessageBase):

    def __init__(self, consumer_name):
        super(ConsumerCompletedMessage, self).__init__()
        self.consumer_name = consumer_name


class OperatorCompletedMessage(MessageBase):

    def __init__(self, name):
        super(OperatorCompletedMessage, self).__init__()
        self.name = name


class EvalMessage(MessageBase):

    def __init__(self, expr):
        super(EvalMessage, self).__init__()
        self.expr = expr


class EvaluatedMessage(MessageBase):

    def __init__(self, val):
        super(EvaluatedMessage, self).__init__()
        self.val = val

class Operator(HandlerBase):
    """Base class for an operator. An operator is a class that can receive tuples from other
    operators (a.k.a. producers) and send tuples to other operators (a.k.a. consumers).

    """
    system = None  # type: WorkerSystem

    def set_async(self, parallel):
        self.async_ = parallel

    def work(self, queue):
        if self.is_profiled:
            cProfile.runctx('self.do_work(queue)', globals(), locals(), self.profile_file_name)
        else:
            self.do_work(queue)

    def do_work(self, queue):
        running = True

        try:
            while running:

                self.op_metrics.timer_stop()
                p_item = queue.get()
                self.op_metrics.timer_start()

                item = pickle.loads(p_item)

                # print(item)

                if type(item) == StartMessage:
                    self.run()
                elif type(item) == StopMessage:
                    break
                elif type(item) == ProducerCompletedMessage:
                    self.on_producer_completed(item.producer_name)
                elif type(item) == ConsumerCompletedMessage:
                    self.on_consumer_completed(item.consumer_name)
                elif type(item) == EvalMessage:
                    evaluated = eval(item.expr)
                    p_evaluated = pickle.dumps(EvaluatedMessage(evaluated))
                    del evaluated
                    evaled_chucksize = 1024 * 1
                    self.completion_queue.put(math.ceil(len(p_evaluated) / evaled_chucksize))
                    for offset in range(0, len(p_evaluated), evaled_chucksize):
                        self.completion_queue.put(p_evaluated[offset: offset + evaled_chucksize])
                    # self.completion_queue.put(p_evaluated)
                else:
                    message = item[0]
                    sender = item[1]

                    self.on_receive(message, sender)

        except BaseException as e:
            tb = traceback.format_exc(e)
            print(tb)
            self.exception = e
            running = False


    def on_message(self, item, worker):
        try:

            running = True

            if isinstance(item, StartMessage):
                self.run()
            elif isinstance(item, StopMessage):
                running = False
            elif isinstance(item, ProducerCompletedMessage):
                self.on_producer_completed(item.producer_name)
            elif isinstance(item, ConsumerCompletedMessage):
                self.on_consumer_completed(item.consumer_name)
            elif isinstance(item, EvalMessage):
                evaluated = eval(item.expr)
                self.system.send('system', EvaluatedMessage(evaluated), self.worker)

                # p_evaluated = pickle.dumps(EvaluatedMessage(evaluated))
                # self.completion_queue.put(p_evaluated)
            else:
                # message = item[0]
                # sender = item[1]

                self.on_receive(item, item.sender_name)

            return running
        except BaseException as e:
            tb = traceback.format_exc(e)
            print(tb)
            self.exception = e
            running = False

    def run(self):
        """Abstract method for running the execution of this operator. This is different from the start method which
        is for sending a start message to this process.

        Only active operators need to implement this, reactive operators (ones that simply listen for tuples are never
        "run")

        :return: None
        """

        pass

    def start(self):
        """Sends a start message to this operators process or calls run if the operator is not async

        :return: None
        """

        if self.async_:
            if self.use_shared_mem:
                self.system.send(self.name, StartMessage(), None)
            else:
                # m = cPickle.dumps(StartMessage())
                m = pickle.dumps(StartMessage())
                self.queue.put(m)
        else:
            self.run()

    def __init__(self, name, op_metrics, query_plan, log_enabled=False):
        """Constructs a new operator

        """
        super(Operator, self).__init__()
        self.name = name

        self.op_metrics = op_metrics

        self.log_enabled = log_enabled

        self.producers = []
        self.consumers = []
        self.tagged_consumers = {}

        self.exception = None

        # TODO: Tidy up how completions are handled
        self.producer_completions = {}
        for p in self.producers:
            self.producer_completions[p.name] = False

        self.consumer_completions = {}
        for p in self.producers:
            self.consumer_completions[p.name] = False

        self.__completed = False

        self.__buffer = []

        # self.is_streamed = True

        self.is_profiled = False
        self.profile_file_name = None

        # Default to 1024 element buffer, use 0 to send immediately, and float('inf') for unlimited buffer
        self.buffer_size = 1024

        self.query_plan = query_plan

        self.async_ = query_plan.is_async

        self.queue = None

        self.completion_queue = None

        self.runner = None

        self.use_shared_mem = False
        self.worker = None
        self.system = None

        self.__buffers = {}
        self.buffered_size = 0

    def init_async(self, completion_queue, system, use_shared_mem):

        # self.async_ = True
        self.use_shared_mem = use_shared_mem

        self.completion_queue = completion_queue

        if self.async_:
            if self.use_shared_mem:
                self.system = system
                self.worker = self.system.create_worker(self.name, self, self.system.channel.buffer_size, self.is_profiled, self.profile_file_name)
            else:
                self.queue = multiprocessing.Queue()
                # self.runner = threading.Thread(target=self.work, args=(self.queue, ))
                self.runner = multiprocessing.Process(target=self.work, args=(self.queue, ))
                self.runner.daemon = True


    def boot(self):
        if self.async_:
            if not self.use_shared_mem:
                self.runner.start()
            else:
                self.start()
        else:
            pass
            # NOOP

    def is_completed(self):
        """Accessor for completed status.

        :return: Boolean indicating whether the operator has completed or not.
        """
        return self.__completed

    def connect(self, consumer, tag=0):
        """Utility method that appends the given consuming operators to this operators list of consumers and appends the
        given consumers producer to this operator. Shorthand for two add consumer, add producer calls.

        TODO: Not sure if this should be here, sometimes consuming operators don't need a ref to a producer or indeed
        may have multiple.

        :param consumer: An operator that will consume the results of this operator.
        :param tag: the tag for the consumer.
        :return: None
        """

        self.add_consumer(consumer, tag)
        consumer.add_producer(self)

    def set_completed(self, completed):
        """
        Completed status setter
        :return:
        """
        self.__completed = completed

    def add_consumer(self, consumer, tag):
        """Appends the given consuming operator to this operators list of consumers

        :param consumer: An operator that will consume the results of this operator.
        :return: None
        """

        if self.log_enabled:
            print("{} | {}('{}') | Adding consumer '{}'"
                  .format(time.time(), self.__class__.__name__, self.name, consumer))

        if any(consumer.name == name for name in self.consumers):
            raise Exception("Consumer with name '{}' already added".format(consumer.name))

        self.consumers.append(consumer)
        self.consumer_completions[consumer.name] = False
        self.consumers = sorted(self.consumers, key=lambda c: c.name)
        if not tag in self.tagged_consumers.keys():
            self.tagged_consumers[tag] = []
        self.tagged_consumers[tag].append(consumer)
        self.tagged_consumers[tag] = sorted(self.tagged_consumers[tag], key=lambda c: c.name)

        self.__buffers[consumer] = []

    def add_producer(self, producer):
        """Appends the given producing operator to this operators list of producers.

        :param producer: An operator that will produce the tuples for this operator.
        :return: None
        """

        if self.log_enabled:
            print("{} | {}('{}') | Adding producer '{}'"
                  .format(time.time(), self.__class__.__name__, self.name, producer))

        if any(producer.name == name for name in self.producers):
            raise Exception("Producer with name '{}' already added".format(producer.name))

        self.producers.append(producer)
        self.producer_completions[producer.name] = False
        self.producers = sorted(self.producers, key=lambda p: p.name)

    def join(self):
        if self.async_:
            self.runner.join()
        else:
            pass

    def set_query_plan(self, query_plan):
        self.query_plan = query_plan

    def send(self, message, operators):
        """Emits the given tuple to each of the connected consumers.

        :param operators:
        :param message: The message to emit
        :return: None
        """

        if not isinstance(message, MessageBase):
            raise Exception("Message type {} does not extend {}".format(message, MessageBase.__class__.__name__))

        if len(operators) == 0:
            raise Exception("Operator {} has 0 consumers. Cannot send message to 0 consumers.".format(self.name))

        if self.buffer_size == 0:
            for op in operators:
                if op.async_:
                    self.query_plan.send([[message], self.name], op.name, self)
                else:
                    self.fire_on_receive([message], op)

        else:
            for o in operators:
                self.__buffers[o].append(message)
                self.buffered_size += 1

            if self.buffered_size >= self.buffer_size:
                for (o, messages) in self.__buffers.items():
                    self.do_send(messages, o)
                    self.__buffers[o] = []

                self.buffered_size = 0

    def do_send(self, messages, op):

        if len(messages) > 0:
            if op.async_:
                self.query_plan.send([messages, self.name], op.name, self)
            else:
                self.fire_on_receive(messages, op)

    def fire_on_receive(self, message, consumer):
        switch_context(self, consumer)
        consumer.on_receive(message, self.name)
        switch_context(consumer, self)

    def on_receive(self, message, producer_name):
        raise NotImplementedError

    def fire_on_producer_completed(self, consumer):
        switch_context(self, consumer)
        consumer.on_producer_completed(self.name)
        switch_context(consumer, self)

    def fire_on_consumer_completed(self, producer):
        switch_context(self, producer)
        producer.on_consumer_completed(self.name)
        switch_context(producer, self)

    def flush(self):
        for (o, messages) in self.__buffers.items():
            self.do_send(messages, o)
            self.__buffers[o] = []

        self.buffered_size = 0

    def complete(self):
        """Sets the operator to complete, meaning it has completed what it needed to do. This includes marking the
        operator as completed and signalling to to connected operators that this operator has
        completed what it was doing.

        :return: None
        """

        if not self.is_completed():

            if self.log_enabled:
                print("{} | {}('{}') | Completed".format(time.time(), self.__class__.__name__, self.name))

            if self.use_shared_mem:
                self.system.send('system', OperatorCompletedMessage(self.worker.name), self.worker)
            else:
                # p_msg = cPickle.dumps(OperatorCompletedMessage(self.name))
                p_msg = pickle.dumps(OperatorCompletedMessage(self.name))
                self.completion_queue.put(p_msg)

            self.__completed = True

            # for (o, messages) in self.__buffers.items():
            #     self.do_send( messages, o)
            #     self.__buffers[o.name] = []

            # Flush the buffer
            self.flush()
            # for c in self.consumers:
            #     if c.async_:
            #         self.query_plan.send([self.__buffer, self.name], c.name)
            #     else:
            #         self.fire_on_receive(self.__buffer, c)
            #
            # self.__buffer = []

            for p in self.producers:
                if p.async_:
                    # p.queue.put(cPickle.dumps(ConsumerCompletedMessage(self.name)))
                    self.query_plan.send(ConsumerCompletedMessage(self.name), p.name, self)
                else:
                    self.fire_on_consumer_completed(p)

            for c in self.consumers:
                if c.async_:
                    # c.queue.put(cPickle.dumps(ProducerCompletedMessage(self.name)))
                    self.query_plan.send(ProducerCompletedMessage(self.name), c.name, self)
                else:
                    self.fire_on_producer_completed(c)

        else:
            raise Exception("Cannot complete an already completed operator")

    # def set_streamed(self, is_streamed):
    #     self.is_streamed = is_streamed

    def set_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size

    def set_profiled(self, is_profiled, profile_file_name=None):
        self.is_profiled = is_profiled
        self.profile_file_name = profile_file_name

    def on_producer_completed(self, producer_name):
        """Handles a signal from producing operators that they have completed what they needed to do. This is useful in
        circumstances where a producer has no more tuples to supply (such as completion of a table scan). This is often
        overridden but this default implementation simply completes this operator.

        :param producer_name: The producer that has completed
        :return: None
        """

        self.producer_completions[producer_name] = True

        # Check if all producers are completed
        if all(self.producer_completions.values()):
            if not self.is_completed():
                self.complete()

    def on_consumer_completed(self, consumer_name):
        """Handles a signal from consuming operators that they have completed what they needed to do. This is useful in
        circumstances where a consumer needs no more tuples (such as a top operator reaching the number of tuples it
        needs). This is often overridden but this default implementation simply simply completes this operator.

        :param consumer_name: The consumer that has completed
        :return: None
        """

        self.consumer_completions[consumer_name] = True

        # Check if all consumers are completed
        if all(self.consumer_completions.values()):
            if not self.is_completed():
                self.complete()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {'name': self.name, 'parallel': self.async_})
