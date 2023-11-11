# -*- coding: utf-8 -*-
"""Bloom filter creation support

"""
import warnings

from s3filter.hash.sliced_sql_bloom_filter import SlicedSQLBloomFilter, MAX_S3_SELECT_EXPRESSION_LEN
from s3filter.multiprocessing.message import DataFrameMessage
from s3filter.op.tuple import IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage, BloomMessage
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.hash.sliced_bloom_filter import SlicedBloomFilter
# noinspection PyCompatibility,PyPep8Naming
# import cPickle as pickle
import pickle
import pandas as pd


class BloomCreateMetrics(OpMetrics):
    """Extra metrics

    """

    def __init__(self):
        super(BloomCreateMetrics, self).__init__()
        self.tuple_count = 0
        self.bloom_filter_capacity = 0
        self.bloom_filter_fp_rate = 0.0
        self.bloom_filter_num_bits_set = 0
        self.bloom_filter_num_slices = 0
        self.bloom_filter_num_bits_per_slice = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'tuple_count': self.tuple_count,
            'bloom_filter_capacity': self.bloom_filter_capacity,
            'bloom_filter_fp_rate': self.bloom_filter_fp_rate,
            'bloom_filter_num_bits_set': self.bloom_filter_num_bits_set,
            'bloom_filter_num_slices': self.bloom_filter_num_slices,
            'bloom_filter_num_bits_per_slice': self.bloom_filter_num_bits_per_slice
        }.__repr__()


class BloomCreate(Operator):
    """This operator creates a bloom filter from the tuples it receives. Given a field name to create the filter from
    it will add the tuple value corresponding the field name to the internal bloom filter. Once the connected
    producer is complete the bloom filter is sent to any connected consumers.

    """

    BLOOM_FILTER_FP_RATE = 0.3

    def __init__(self, bloom_field_name, name, query_plan, log_enabled, fp_rate=BLOOM_FILTER_FP_RATE):
        """

        :param bloom_field_name: The tuple field name to extract values from to create the bloom filter
        :param name: The operator name
        :param log_enabled: Logging enabled
        """

        super(BloomCreate, self).__init__(name, BloomCreateMetrics(), query_plan, log_enabled)

        self.bloom_field_name = bloom_field_name

        self.__field_names = None

        self.__tuples = []

        self.producer_completions = {}

        self.producers_received = {}

        self.fp_rate = fp_rate

        # These settings are similar to the simple version
        # self.__bloom_filter = ScalableBloomFilter(64, 0.75, ScalableBloomFilter.LARGE_SET_GROWTH)

        # These settings are similar to the simple version
        # self.build_bloom_filter(8192, 0.75)

        # The simple filter
        # self.__bloom_filter = SimpleBloomFilter()

    def build_bloom_filter(self, capacity, fp_rate):
        bloom_filter = SlicedBloomFilter(capacity, fp_rate)

        self.op_metrics.bloom_filter_capacity = bloom_filter.capacity
        self.op_metrics.bloom_filter_fp_rate = bloom_filter.error_rate
        self.op_metrics.bloom_filter_num_slices = bloom_filter.num_slices
        self.op_metrics.bloom_filter_num_bits_per_slice = bloom_filter.num_bits_per_slice
        self.op_metrics.bloom_filter_capacity = bloom_filter.capacity

        return bloom_filter

    def connect(self, consumer, tag=0):
        """Overrides the generic connect method to make sure that the connecting operator is an operator that consumes
        bloom filters.

        :param consumer: The consumer to connect
        :return: None
        """

        if type(consumer) is not SQLTableScanBloomUse:
            raise Exception("Illegal consumer. {} operator may only be connected to {} operators"
                            .format(self.__class__.__name__, SQLTableScanBloomUse.__name__))

        Operator.connect(self, consumer)

    def on_receive(self, ms, producer_name):
        """Event handler for receiving a message

        :param ms: The messages
        :param producer_name: The producer that sent the message
        :return: None
        """
        for m in ms:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_, producer_name)
            elif isinstance(m, DataFrameMessage):
                self.__on_receive_dataframe(m.dataframe, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_producer_completed(self, producer_name):
        """Event handler for a completed producer. When producers complete the bloom filter can be sent.

        :param producer_name: The producer that completed.
        :return: None
        """

        self.producer_completions[producer_name] = True

        if all(self.producer_completions.values()):

            # Get the SQL from a bloom use operators
            bloom_use_operators = filter(lambda o: isinstance(o, SQLTableScanBloomUse), self.consumers)
            bloom_use_sql_strings = map(lambda o: o.s3sql, bloom_use_operators)
            max_bloom_use_sql_strings = max(map(lambda s: len(s), bloom_use_sql_strings))

            # Build bloom filter
            best_possible_fp_rate = SlicedSQLBloomFilter.calc_best_fp_rate(len(self.__tuples),
                                                                           max_bloom_use_sql_strings)

            if best_possible_fp_rate > self.fp_rate:
                print("{}('{}') | Bloom filter fp rate ({}) too low, "
                              "will exceed max S3 Select SQL expression length ({}). "
                              "Raising to best possible ({})".format(self.__class__.__name__,
                                                                     self.name,
                                                                     self.fp_rate,
                                                                     MAX_S3_SELECT_EXPRESSION_LEN,
                                                                     best_possible_fp_rate))
                fp_rate_to_use = best_possible_fp_rate
            else:
                fp_rate_to_use = self.fp_rate

            bloom_filter = self.build_bloom_filter(len(self.__tuples), fp_rate_to_use)

            for t in self.__tuples:
                lt = IndexedTuple.build(t, self.__field_names)
                bloom_filter.add(int(lt[self.bloom_field_name]))

            del self.__tuples

            # Send the bloom filter
            self.__send_bloom_filter(bloom_filter)

        Operator.on_producer_completed(self, producer_name)

    def __send_bloom_filter(self, bloom_filter):
        """Sends the bloom filter to connected consumers.

        :return: None
        """

        if self.log_enabled:
            print("{}('{}') | Sending bloom filter [{}]".format(
                self.__class__.__name__,
                self.name,
                {'bloom_filter': bloom_filter}))

        self.op_metrics.bloom_filter_num_bits_set = len(bloom_filter)

        self.send(BloomMessage(bloom_filter), self.consumers)

    def __on_receive_tuple(self, tuple_, producer_name):
        """Event handler for receiving a tuple

        :param tuple_: The received tuple
        :return: None
        """

        if self.__field_names is None:

            if self.bloom_field_name not in tuple_:
                raise Exception(
                    "Received invalid tuple {} from {}. "
                    "Tuple field names '{}' do not contain field with bloom field name '{}'"
                        .format(tuple_, producer_name, tuple_, self.bloom_field_name))

            # Don't send the field names, just collect them
            self.__field_names = tuple_

            self.producers_received[producer_name] = True

        else:

            if producer_name not in self.producers_received.keys():
                # This will be the field names tuple, skip it
                self.producers_received[producer_name] = True
            else:
                self.__tuples.append(tuple_)
                self.op_metrics.tuple_count += 1

                # lt = IndexedTuple.build(tuple_, self.__field_names)
                #
                # # NOTE: Bloom filter only supports ints. Not clear how to make it support strings as yet
                # self.__bloom_filter.add(int(lt[self.__bloom_field_name]))

    def __on_receive_dataframe(self, df, producer_name):
        """Event handler for receiving a dataframe

        :param df: The received dataframe
        :return: None
        """

        if self.__field_names is None:

            if self.bloom_field_name not in df.columns.values:
                raise Exception(
                    "Received invalid tuple {} from {}. "
                    "Tuple field names '{}' do not contain field with bloom field name '{}'"
                        .format(tuple_, producer_name, tuple_, self.bloom_field_name))

            # Don't send the field names, just collect them
            self.__field_names = df.columns.values

            self.producers_received[producer_name] = True

        if producer_name not in self.producers_received.keys():
            # This will be the field names tuple, skip it
            self.producers_received[producer_name] = True

        # TODO directly compute on df
        for t in df.values.tolist():
            self.__tuples.append(t)
            self.op_metrics.tuple_count += 1
