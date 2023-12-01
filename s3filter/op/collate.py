# -*- coding: utf-8 -*-
"""Collation support

"""
# noinspection PyCompatibility,PyPep8Naming
# import cPickle as pickle
import pickle
import sys

import pandas as pd
from pandas import DataFrame

from s3filter.multiprocessing.message import DataFrameMessage
from s3filter.op.message import TupleMessage, StringMessage
from s3filter.op.operator_base import Operator, EvalMessage, EvaluatedMessage
from s3filter.plan.op_metrics import OpMetrics

import datetime 
import pytz
import random


class Collate(Operator):
    """This operator simply collects emitted tuples into a list. Useful for interrogating results at the end of a
    query to see if they are what is expected.

    """

    def __init__(self, name, query_plan, log_enabled):
        """Constructs a new Collate operator.

        """

        super(Collate, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.__tuples = []

        self.df = DataFrame()

        self.counter = 0

        self.total_time_on_receive = 0
        

    def tuples(self):
        """Accessor for the collated tuples

        :return: The collated tuples
        """

        print("testtest: invoke tuples")
        print(datetime.datetime.now(pytz.timezone('America/Chicago')))

        if self.async_:
            if self.use_shared_mem:
                self.system.send(self.name, EvalMessage("self.local_tuples()"), self.worker)
            else:
                p_message = pickle.dumps(EvalMessage("self.local_tuples()"))
                self.queue.put(p_message)

            item = self.query_plan.listen(EvaluatedMessage)
            tuples = item.val

            print("testtest: got tuples")
            print(datetime.datetime.now(pytz.timezone('America/Chicago')))
            return tuples
        else:
            return self.__tuples

    def local_tuples(self):
        """Accessor for the collated tuples

        :return: The collated tuples
        """

        return self.__tuples

    def on_receive(self, ms, _producer):
        """Handles the event of receiving a message from a producer. Will simply append the tuple to the internal
        list.

        :param ms: The received messages
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Collate | {}".format(t))
        # print("testtest: collate on receive")
        # print(datetime.datetime.now(pytz.timezone('America/Chicago')))

        # for m in ms:
        if self.use_shared_mem:
            self.on_receive_message(ms)
        else:
            for m in ms:
                self.on_receive_message(m)

    def on_receive_message(self, m):
        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        elif isinstance(m, DataFrameMessage):
            self.__on_receive_dataframe(m.dataframe)
        elif isinstance(m, StringMessage):
            print(m.string_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def __on_receive_dataframe(self, df):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        # TODO: Also adding to tuples for now just so the existing tests work,
        # eventually they should inspect the dataframe

        start_time = datetime.datetime.now()

        self.df = pd.concat([self.df, df])
        self.__tuples = [list(self.df)] + self.df.values.tolist()

        time_diff = datetime.datetime.now() - start_time
        
        # randomly print time_diff
        if self.counter % 100 == 0:
            print("testtest: __on_receive_dataframe timediff")
            print(time_diff.total_seconds())
        self.counter = self.counter + 1

        print("testtest: __on_receive_dataframe total timediff")
        self.total_time_on_receive = self.total_time_on_receive + time_diff.total_seconds()
        print(self.total_time_on_receive)
        print("testtest: __on_receive_dataframe total counts")
        print(self.counter)


        #self.__tuples.extend(df.values.tolist())
        #self.df = self.df.append(df)

    def __on_receive_tuple(self, tuple_):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        self.__tuples.append(tuple_)

    def print_tuples(self, tuples=None):
        """Prints the tuples in tab separated format

        TODO: Needs some work to align properly

        :return:
        """

        print('')

        # self.write_to(sys.stdout, tuples)
        with open("/home/ubuntu/s3filter_cloud/output.csv", 'w') as file:
            file.write('\n'.join(map(str, tuples)))

    def write_to(self, out_stream, tuples=None):

        if tuples is None:
            tuples = self.__tuples

        field_names = False
        for t in tuples:
            if not field_names:

                # Write field name tuple

                field_names_len = 0
                t_iter = iter(t)
                first_field_name = True
                for f in t_iter:
                    if not first_field_name:
                        out_stream.write(' | ')
                    else:
                        first_field_name = False

                    out_stream.write(str(f))
                    field_names_len += len(str(f))

                out_stream.write('\n')
                out_stream.write('-' * (field_names_len + ((len(t) - 1) * len(' | '))))
                out_stream.write('\n')
                field_names = True
            else:
                t_iter = iter(t)
                first_field_val = True
                for f in t_iter:
                    if not first_field_val:
                        out_stream.write(' | ')
                    else:
                        first_field_val = False

                    out_stream.write(str(f))
                out_stream.write('\n')
