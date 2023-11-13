"""Top K baseline

"""
import os

import numpy as np

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sort import SortExpression
from s3filter.op.sql_table_scan import SQLTableScan
# from s3filter.op.top import Top
from s3filter.op.top import DummyTop
from s3filter.op.top_filter_build import TopKFilterBuild
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id


def main():
    path = 'access_method_benchmark/shards-1GB'
    k = 100
    queried_columns = ['l_orderkey', 'l_extendedprice']
    select_columns = ", ".join(queried_columns)
    if len(queried_columns) == 16:
        select_columns = "*"

    run('l_extendedprice', k, sample_size=5000, parallel=True, use_pandas=True,
        sort_order='ASC', buffer_size=0, table_first_part=1, queried_columns=queried_columns,
        select_columns=select_columns, table_parts=2, path=path, format_= Format.CSV)

def run(sort_field, k, sample_size, parallel, use_pandas, sort_order, buffer_size, table_first_part, table_parts,
         queried_columns, select_columns, path, format_):
    """
    Executes the baseline topk query by scanning a table and keeping track of the max/min records in a heap
    :return:
    """

    secure = False
    use_native = False
    print('')
    print("Top K Benchmark, Sampling. Sort Field: {}, Order: {}".format(sort_field, sort_order))
    print("----------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    sample_topk = query_plan.add_operator(
        DummyTop(path=path, k=k, sort_order=sort_order, sort_field=sort_field, 
            queried_columns='|'.join(queried_columns), 
            table_first_part=table_first_part, table_parts=table_parts, sample_size=sample_size, 
            use_pandas=use_pandas, secure=secure, use_native=use_native, name='topk_local_dummy', query_plan=query_plan, log_enabled=False)
        ) 
    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    sample_topk.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("table parts: {}".format(table_parts))
    print('')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(table_parts))

    # Start the query
    query_plan.execute()
    print('Done')
    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

if __name__ == "__main__":
    main()
