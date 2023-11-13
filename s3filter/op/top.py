from datetime import date

from s3filter.multiprocessing.message import DataFrameMessage
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator, EvalMessage, EvaluatedMessage
from s3filter.op.message import TupleMessage, DataFrameMessage
from s3filter.op.sort import SortExpression
from s3filter.op.sql_table_scan import SQLTableScanMetrics, is_header
from s3filter.plan.query_plan import QueryPlan
from s3filter.plan.cost_estimator import CostEstimator
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_sharded_table_scan import SQLShardedTableScan
from s3filter.op.collate import Collate
from s3filter.util.heap import MaxHeap, MinHeap, HeapTuple
import time
import sys
from collections import Iterable
import pandas as pd

import boto3
import json


__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class Top(Operator):
    """
    Implementation of the TopK operator based on user selected sorting criteria and expressions. This operator
    consumes tuples from producer operators and uses a heap to keep track of the top k tuples.
    """

    def __init__(self, max_tuples, sort_expression, use_pandas, name, query_plan, log_enabled):
        """Creates a new Sort operator.

                :param sort_expression: The sort expression to apply to the tuples
                :param name: The name of the operator
                :param log_enabled: Whether logging is enabled
                """

        super(Top, self).__init__(name, OpMetrics(), query_plan, log_enabled)

        self.sort_expression = sort_expression if isinstance(sort_expression, Iterable) else [sort_expression]
        self.use_pandas = use_pandas

        if not self.use_pandas:
            if sort_expression.sort_order == 'ASC':
                self.heap = MaxHeap(max_tuples)
            else:
                self.heap = MinHeap(max_tuples)
        else:
            self.global_topk_df = pd.DataFrame()

        self.max_tuples = max_tuples

        self.field_names = None

    def on_receive(self, ms, _producer):
        """Handles the receipt of a message from a producer.

        :param ms: The received messages
        :param _producer: The producer that emitted the message
        :return: None
        """
        for m in ms:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_, _producer)
            elif isinstance(m, DataFrameMessage):
                self.__on_receive_dataframe(m.dataframe, _producer)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, producer_name):
        """Handles the receipt of a tuple. When a tuple is received, it's compared with the top of the heap to decide
        on adding to the heap or skip it. Given this process, it is guaranteed to keep the k topmost tuples given some
        defined comparison criteria

        :param tuple_: The received tuple
        :return: None
        """
        pass
        #if not self.field_names:
        #    # Collect and send field names through
        #    self.field_names = tuple_
        #    #self.send(TupleMessage(tuple_), self.consumers)
        #elif not is_header(tuple_):
        #    # Store the tuple in the sorted heap
        #    ht = HeapTuple(tuple_, self.field_names, self.sort_expression)
        #    self.heap.push(ht)

    def __on_receive_dataframe(self, df, producer_name):
        """Event handler for a received pandas dataframe. With every dataframe received, the topk tuples are mantained
        and merged

        :param df: The received dataframe
        :return: None
        """
        if len(df) == 0:
            return

        if len(self.sort_expression) == 1:
            sort_expr = self.sort_expression[0]
            df[[sort_expr.col_index]] = df[[sort_expr.col_index]]\
                                                       .astype(sort_expr.col_type.__name__)

            if sort_expr.sort_order == 'ASC':
                topk_df = df.nsmallest(self.max_tuples, sort_expr.col_index).head(self.max_tuples)
                self.global_topk_df = self.global_topk_df.append(topk_df).nsmallest(self.max_tuples,
                                                                                    sort_expr.col_index) \
                                                                         .head(self.max_tuples)
            elif sort_expr.sort_order == 'DESC':
                topk_df = df.nlargest(self.max_tuples, sort_expr.col_index).head(self.max_tuples)
                self.global_topk_df = self.global_topk_df.append(topk_df).nlargest(self.max_tuples, sort_expr.col_index) \
                                                                         .head(self.max_tuples)
        elif len(self.sort_expression) > 1:
            # Support for multiple sort expressions
            for se in self.sort_expression:
                if se.col_type is date:
                    df[se.col_index] = pd.to_datetime(df[se.col_index])
                else:
                    df[se.col_index] = df[se.col_index].astype(se.col_type.__name__)

            by = [se.col_index for se in self.sort_expression]
            ascending = [se.sort_order == 'ASC' for se in self.sort_expression]

            topk_df = df.sort_values(by=by, ascending=ascending).head(self.max_tuples)
            self.global_topk_df = self.global_topk_df.append(topk_df).sort_values(by=by, ascending=ascending).head(self.max_tuples)

    def on_producer_completed(self, producer_name):
        if producer_name in self.producer_completions.keys():
            self.producer_completions[producer_name] = True
        else:
            raise Exception("Unrecognized producer {} has completed".format(producer_name))

        is_all_producers_done = all(self.producer_completions.values())
        if not is_all_producers_done:
            return

        # NOTE: Not sure if this is necessary as the global df is sorted on receipt of each dataframe

        # if not self.use_pandas:
        #     raise Exception("TopK only supports pandas right now")
        # elif len(self.global_topk_df) > 0:
        #     if self.sort_expression.sort_order == 'ASC':
        #         self.global_topk_df = self.global_topk_df.nsmallest(self.max_tuples, self.sort_expression.col_index) #.head(self.max_tuples)
        #     elif self.sort_expression.sort_order == 'DESC':
        #         self.global_topk_df = self.global_topk_df.nlargest(self.max_tuples, self.sort_expression.col_index) #.head(self.max_tuples)

        self.send(DataFrameMessage(self.global_topk_df), self.consumers)

        Operator.on_producer_completed(self, producer_name)


class TopKTableScanMetrics(SQLTableScanMetrics):

    def __init__(self):
        super(TopKTableScanMetrics, self).__init__()

        self.sampling_time = 0.0
        self.sampling_data_cost = 0.0
        self.sampling_computation_cost = 0.0

    def cost(self):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated cost of the table scan operation
        """
        return self.sampling_computation_cost + self.sampling_data_cost + \
               super(TopKTableScanMetrics, self).cost()

    def computation_cost(self, running_time=None, ec2_instance_type=None, os_type=None):
        """
        Estimates the computation cost of the scan operation based on EC2 pricing in the following page:
        <https://aws.amazon.com/ec2/pricing/on-demand/>
        :param running_time: the query running time
        :param ec2_instance_type: the type of EC2 instance as defined by AWS
        :param os_type: the name of the os running on the host machine (Linux, Windows ... etc)
        :return: The estimated computation cost of the table scan operation given the query running time
        """
        return self.sampling_computation_cost + \
               super(TopKTableScanMetrics, self).computation_cost(running_time, ec2_instance_type, os_type)

    def data_cost(self, ec2_region=None):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated data transfer cost of the table scan operation
        """
        return self.sampling_data_cost + \
               super(TopKTableScanMetrics, self).data_cost(ec2_region)


class TopKTableScan(Operator):
    """
    This operator scans a table and emits the k topmost tuples based on a user-defined ranking criteria
    """

    def __init__(self, s3key, s3sql, use_pandas, secure, use_native, max_tuples, k_scale, sort_expression,
                 is_conservative, shards_start, shards_end, parallel_shards, shards_prefix, processes, name, query_plan,
                 log_enabled):
        """
        Creates a table scan operator that emits only the k topmost tuples from the table
        :param s3key: the table's s3 object key
        :param s3sql: the select statement to apply on the table
        :param use_pandas: use pandas DataFrames as the tuples engine
        :param use_native: use native C++ cursor
        :param max_tuples: the maximum number of tuples to return (K)
        :param k_scale: sampling scale factor to retrieve more sampling tuples (s * K)
        :param sort_expression: the expression on which the table tuples are sorted in order to get the top k
        :param name: the operator name
        :param query_plan: the query plan in which this operator is part of
        :param log_enabled: enable logging
        """
        super(TopKTableScan, self).__init__(name, TopKTableScanMetrics(), query_plan, log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql
        self.query_plan = query_plan
        self.use_pandas = use_pandas
        self.secure = secure
        self.use_native = use_native
        self.is_conservative = is_conservative

        self.field_names = None

        self.shards = shards_end - shards_start + 1
        self.parallel_shards = parallel_shards
        self.processes = processes

        self.max_tuples = max_tuples
        self.sort_expression = sort_expression

        if not self.use_pandas:
            if sort_expression.sort_order == 'ASC':
                self.heap = MaxHeap(max_tuples)
            else:
                self.heap = MinHeap(max_tuples)
        else:
            self.global_topk_df = pd.DataFrame()

        self.local_operators = []

        # TODO: The current sampling method has a flaw. If the maximum value happened to be among the sample without
        # duplicates, scanning table shards won't return any tuples and the sample will be considered the topk while
        # this is not necessarily the correct topk. A suggestion is to take a random step back on the cutting threshold
        # to make sure the max value is not used as the threshold
        self.sample_tuples, self.sample_op, q_plan = TopKTableScan.sample_table(self.s3key, k_scale * self.max_tuples,
                                                                        self.sort_expression)
        # self.field_names = self.sample_tuples[0]
        self.lsv, self.msv, comp_op = self.get_significant_values(self.sample_tuples[1:])

        self.op_metrics.sampling_data_cost = q_plan.data_cost()[0]
        self.op_metrics.sampling_computation_cost = q_plan.computation_cost()
        self.op_metrics.sampling_time = q_plan.total_elapsed_time

        if self.is_conservative:
            threshold = self.lsv
        else:
            threshold = self.msv

        filtered_sql = "{} WHERE CAST({} AS {}) {} {};".format(self.s3sql.rstrip(';'), self.sort_expression.col_name,
                                                               self.sort_expression.col_type.__name__, comp_op,
                                                               threshold)

        if self.processes == 1:
            ts = SQLTableScan(self.s3key, filtered_sql, self.use_pandas, self.secure, self.use_native,
                              'baseline_topk_table_scan',
                              self.query_plan, self.log_enabled)
            ts.connect(self)
            self.local_operators.append(ts)
        else:
            for process in range(self.processes):
                proc_parts = [x for x in range(shards_start, shards_end + 1) if x % self.processes == process]
                pc = self.query_plan.add_operator(SQLShardedTableScan(self.s3key, filtered_sql, self.use_pandas,
                                                                      self.secure,
                                                                      self.use_native,
                                                                      "topk_table_scan_parts_{}".format(proc_parts),
                                                                      proc_parts, shards_prefix,
                                                                      self.parallel_shards,
                                                                      self.query_plan, self.log_enabled))
                proc_top = self.query_plan.add_operator(Top(self.max_tuples, self.sort_expression, True,
                                                            "top_parts_{}".format(proc_parts), self.query_plan,
                                                            self.log_enabled))
                pc.connect(proc_top)
                proc_top.connect(self)

                if self.query_plan.is_async:
                    pc.init_async(self.query_plan.queue)
                    proc_top.init_async(self.query_plan.queue)

                self.local_operators.append(pc)
                self.local_operators.append(proc_top)

    def run(self):
        """
        starts the topk query execution to do the following. First, select randomly the first k tuples from
        the designated table. Then, retrieve all the tuples larger/smaller than the max/min of the retrieved tuples set
        to filter them and get the global k topmost tuples. This reduces the search space by taking a random sample
        from the table to start with
        :return:
        """
        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        for op in self.local_operators:
            if self.query_plan.is_async:
                op.boot()
            op.start()

    def on_receive(self, messages, producer_name):
        """Handles the receipt of a message from a producer.

        :param messages: The received messages
        :param producer_name: The producer that emitted the message
        :return: None
        """
        for m in messages:
            if type(m) is TupleMessage:
                self.__on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is pd.DataFrame:
                self.__on_receive_dataframe(m, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_, producer_name):
        """Handles the receipt of a tuple. When a tuple is received, it's compared with the top of the heap to decide
        on adding to the heap or skip it. Given this process, it is guaranteed to keep the k topmost tuples given some
        defined comparison criteria

        :param tuple_: The received tuple
        :return: None
        """
        if not self.field_names:
            # Collect and send field names through
            self.field_names = tuple_
            self.send(TupleMessage(tuple_), self.consumers)
        elif not is_header(tuple_):
            # Store the tuple in the sorted heap
            ht = HeapTuple(tuple_, self.field_names, self.sort_expression)
            self.heap.push(ht)

    def __on_receive_dataframe(self, df, producer_name):
        """Event handler for a received pandas dataframe. With every dataframe received, the topk tuples are mantained
        and merged

        :param df: The received dataframe
        :return: None
        """
        if len(df) > 0:
            df[[self.sort_expression.col_index]] = df[[self.sort_expression.col_index]] \
                                                        .astype(self.sort_expression.col_type.__name__)

            if self.sort_expression.sort_order == 'ASC':
                topk_df = df.nsmallest(self.max_tuples, self.sort_expression.col_index).head(self.max_tuples)
                self.global_topk_df = self.global_topk_df.append(topk_df).nsmallest(self.max_tuples,
                                                                                    self.sort_expression.col_index) \
                                                                                    .head(self.max_tuples)
            elif self.sort_expression.sort_order == 'DESC':
                topk_df = df.nlargest(self.max_tuples, self.sort_expression.col_index).head(self.max_tuples)
                self.global_topk_df = self.global_topk_df.append(topk_df).nlargest(self.max_tuples,
                                                                                   self.sort_expression.col_index) \
                                                                                    .head(self.max_tuples)

    def complete(self):
        """
        When all producers complete, the topk tuples are passed to the next operators.
        :return:
        """
        if not self.use_pandas:
            # if the number of tuples beyond the cut-off value is less than k, we need to some tuples from
            # the sample set
            if len(self.heap) < self.max_tuples:
                self.on_receive([TupleMessage(t) for t in self.sample_tuples], self.name)

            for t in self.heap.get_topk(self.max_tuples, sort=True):
                if self.is_completed():
                    break
                self.send(TupleMessage(t.tuple), self.consumers)

            self.heap.clear()
        else:
            if self.sort_expression.sort_order == 'ASC':
                self.global_topk_df = self.global_topk_df.nsmallest(self.max_tuples, self.sort_expression.col_index) \
                    .head(self.max_tuples)
            elif self.sort_expression.sort_order == 'DESC':
                self.global_topk_df = self.global_topk_df.nlargest(self.max_tuples, self.sort_expression.col_index) \
                    .head(self.max_tuples)

            self.send(self.global_topk_df, self.consumers)

        super(TopKTableScan, self).complete()

        self.op_metrics.timer_stop()

    @staticmethod
    def sample_table(s3key, k, sort_exp):
        """
        Given a table name, return a random sample of records. Currently, the returned records are the first k tuples
        :param s3key: the s3 object name
        :param k: the number of tuples to return (the number added to the SQL Limit clause
        :param sort_exp: the sort expression in which the topk is chosen upon
        :return: the list of selected keys from the first k tuples in the table
        """
        projection = "CAST({} as {})".format(sort_exp.col_name, sort_exp.col_type.__name__)

        sql = "SELECT {} FROM S3Object LIMIT {}".format(projection, k)
        q_plan = QueryPlan(None, is_async=False)
        select_op = q_plan.add_operator(SQLTableScan(s3key, sql, True, True, False, "sample_{}_scan".format(s3key),
                                                  q_plan, False))

        from copy import deepcopy
        sample_topk_sort_exp = deepcopy(sort_exp)
        sample_topk_sort_exp.col_index = '_0'
        topk = q_plan.add_operator(Top(max_tuples=k,
                                       sort_expression=sample_topk_sort_exp,
                                       use_pandas=True,
                                       name="sampling_topk",
                                       query_plan=q_plan,
                                       log_enabled=False))
        collate = q_plan.add_operator(Collate("sample_{}_collate".format(s3key), q_plan, False))
        select_op.connect(topk)
        topk.connect(collate)

        q_plan.execute()

        q_plan.print_metrics()

        return collate.tuples(), select_op, q_plan

    def get_significant_values(self, tuples):
        """
        Returns the cut-off value from the passed tuples in order to retrieve only tuples beyond this point
        :param tuples: the tuples representing the table sample of size k
        :return: the cut-off value
        """
        sort_exp = self.sort_expression
        # idx = self.field_names.index(sort_exp.col_index)

        if sort_exp.sort_order == "ASC":
            min_val, max_val = TopKTableScan.min_max(
                sorted([sort_exp.col_type(t[0]) for t in tuples])[:self.max_tuples])
            # max_val = max(sorted([sort_exp.col_type(t[0]) for t in tuples])[:self.max_tuples])
            # min_val = min(sorted([sort_exp.col_type(t[0]) for t in tuples])[:self.max_tuples])

            return max_val, min_val, '<='
        elif sort_exp.sort_order == 'DESC':
            min_val, max_val = TopKTableScan.min_max(
                sorted([sort_exp.col_type(t[0]) for t in tuples], reverse=True)[:self.max_tuples])
            # max_val = max(sorted([sort_exp.col_type(t[0]) for t in tuples], reverse=True)[:self.max_tuples])
            # min_val = min(sorted([sort_exp.col_type(t[0]) for t in tuples], reverse=True)[:self.max_tuples])

            return min_val, max_val, '>='

        # if sort_exp.sort_order == "ASC":
        #     if self.is_conservative:
        #         return max(sorted([sort_exp.col_type(t[0]) for t in tuples])[:self.max_tuples]), '<='
        #     else:
        #         return min(sorted([sort_exp.col_type(t[0]) for t in tuples])[:self.max_tuples]), '<='
        # elif sort_exp.sort_order == "DESC":
        #     if self.is_conservative:
        #         return min(sorted([sort_exp.col_type(t[0]) for t in tuples], reverse=True)[:self.max_tuples]), '>='
        #     else:
        #         return max(sorted([sort_exp.col_type(t[0]) for t in tuples], reverse=True)[:self.max_tuples]), '>='

    @staticmethod
    def min_max(iterable):
        min_val = sys.maxint
        max_val = -1 * (sys.maxint - 1)

        for n in iterable:
            if n < min_val:
                min_val = n
            if n > max_val:
                max_val = n

        return min_val, max_val


class DummyTopMetrics(OpMetrics):
    """Extra metrics for a lambda function
    """
    def __init__(self):
        super(DummyTopMetrics, self).__init__()
        self.rows_returned = 0
        self.query_bytes = 0
        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0
        self.num_http_get_requests = 0
        self.cost_estimator = CostEstimator(self)

    def cost(self):
        return self.cost_estimator.estimate_cost()

    def computation_cost(self, running_time=None, ec2_instance_type=None, os_type=None):
        """
        Estimates the computation cost of the scan operation based on EC2 pricing in the following page:
        <https://aws.amazon.com/ec2/pricing/on-demand/>
        :param running_time: the query running time
        :param ec2_instance_type: the type of EC2 instance as defined by AWS
        :param os_type: the name of the os running on the host machine (Linux, Windows ... etc)
        :return: The estimated computation cost of the table scan operation given the query running time
        """
        return self.cost_estimator.estimate_computation_cost(running_time, ec2_instance_type, os_type)

    def data_cost(self, ec2_region=None):
        """
        Estimates the cost of the scan operation based on S3 pricing in the following page:
        <https://aws.amazon.com/s3/pricing/>
        :return: The estimated data transfer cost of the table scan operation
        """
        return self.cost_estimator.estimate_data_cost(ec2_region) + self.cost_estimator.estimate_request_cost()

    def data_scan_cost(self):
        """
        Estimate the cost of S3 data scanning
        :return: the estimated data scanning costin USD
        """
        return self.cost_estimator.estimate_data_scan_cost()

    def data_transfer_cost(self, ec2_region=None, s3_region=None):
        """
        Estimate the cost of transferring data either by s3 select or normal data transfer fees
        :param ec2_region: the region where the computing node resides
        :param s3_region: the region where the s3 data is stored in
        :return: the estimated data transfer cost in USD
        """
        return self.cost_estimator.estimate_data_transfer_cost(ec2_region, s3_region)

    def requests_cost(self):
        """
        Estimate the cost of the http GET requests
        :return: the estimated http GET request cost for this particular operation
        """
        return self.cost_estimator.estimate_request_cost()

class DummyTop(Operator):
    """
    The operator simply invoke TopK operator on Lambda, and receive streaming records
    """
    def __init__(self, path, k, sort_order, sort_field, queried_columns, table_first_part, table_parts, sample_size, 
                    use_pandas, secure, use_native, name, query_plan, log_enabled):
        """
        Creates a table scan operator that emits only the k topmost tuples from the table
        :param s3key: the table's s3 object key
        :param use_pandas: use pandas DataFrames as the tuples engine
        :param use_native: use native C++ cursor
        :param max_tuples: the maximum number of tuples to return (K)
        :param k_scale: sampling scale factor to retrieve more sampling tuples (s * K)
        :param sort_expression: the expression on which the table tuples are sorted in order to get the top k
        :param name: the operator name
        :param query_plan: the query plan in which this operator is part of
        :param log_enabled: enable logging
        """
        super(DummyTop, self).__init__(name, DummyTopMetrics(), query_plan, log_enabled)
        # TODO: should include aggregated attr: cost, bytes_returned, bytes_returned, lambda cost relevent metrics
        # traget data
        self.path = path
        self.k = k
        self.table_first_part = table_first_part
        self.table_parts = table_parts
        self.sort_order = sort_order
        self.sort_field = sort_field
        self.queried_columns = queried_columns

        # strategy
        self.sample_size = sample_size

        # config
        self.query_plan = query_plan
        self.use_pandas = use_pandas
        self.secure = secure
        self.use_native = use_native

        # lambda 
        self.lambda_name = "demo2"    # ?????????????????
        self.lambda_client = boto3.client('lambda', region_name='us-east-2')

        # data buffer
        self.global_topk_df = pd.DataFrame()

    def run(self):
        """Executes the query and begins emitting tuples.
        :return: None
        """
        self.op_metrics.timer_start()

        if self.log_enabled:
            print("{} | {}('{}') | Started"
                  .format(time.time(), self.__class__.__name__, self.name))

        # Invoke the Lambda function
        payload = {
            "path": self.path,
            "table_first_part": self.table_first_part,
            "table_parts": self.table_parts,
            "k": self.k,
            "sort_order": self.sort_order,
            "sample_size": self.sample_size,
            "sort_field": self.sort_field,
            "queried_columns": self.queried_columns
        }
        response = self.lambda_client.invoke(
            FunctionName=self.lambda_name,
            InvocationType='RequestResponse',  # Use 'Event' for asynchronous invocation
            Payload=json.dumps(payload))

        # parse Lambda response (not support streaming data)
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))['body']
        response_payload = json.loads(response_payload)

        # convert record to dataframe
        for record in response_payload['data']:
            new_row_df = pd.DataFrame([record])
            self.global_topk_df = pd.concat([self.global_topk_df, new_row_df], ignore_index=True)
        # send records to consumer
        self.send(DataFrameMessage(self.global_topk_df), self.consumers)

        # TODO: parse metrics
        print(response_payload['metrics'])
        self.op_metrics.rows_returned += self.global_topk_df.shape[0]
        # self.op_metrics.bytes_scanned = bytes_scanned
        # self.op_metrics.bytes_returned = bytes_returned
        # self.op_metrics.query_bytes = query_bytes
        # self.op_metrics.lambda_duration = lambda_elapsed_time
        # self.op_metrics.lambda2ec2_bytes = lambda_bytes_returned

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()
        # cur.save_table()  # save to disk, maybe not necessary
