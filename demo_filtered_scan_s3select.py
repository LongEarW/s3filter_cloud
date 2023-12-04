# -*- coding: utf-8 -*-
"""Indexing Benchmark 

"""

import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id
import sys


def main(price_value):
    run(True, True, 0, 10, 0.01, 'access_method_benchmark/shards-1GB', Format.CSV, price_value)


def run(parallel, use_pandas, buffer_size, table_parts, perc, path, format_, price_value):
    secure = False
    use_native = False
    print('')
    print("Indexing Benchmark")
    print("------------------")

    # Query plan
    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Scan Index Files
    # upper = perc * 100
    upper = 0.1
    scan = []
    for p in range(1, table_parts + 1):
        scan.append(query_plan.add_operator(
            SQLTableScan('{}/lineitem.{}.csv'.format(path, p),
                        "select * from S3Object "
                        "where L_EXTENDEDPRICE < '{}';".format(price_value), format_,
                        use_pandas, secure, use_native,
                        'scan_{}'.format(p), query_plan,
                        False)))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))


    for p, opt in enumerate(scan):
        opt.connect(collate)

    # scan = map(lambda p:
    #            query_plan.add_operator(
    #                SQLTableScan('{}/lineitem_{}.csv'.format(path, p),
    #                             "select * from S3Object "
    #                             " where cast(L_DISCOUNT as float) = {};".format(upper), format_,
    #                             use_pandas, secure, use_native,
    #                             'scan_{}'.format(p), query_plan,
    #                             False)),
    #            range(0, table_parts))



    # map(lambda p, o: o.connect(collate), enumerate(scan))

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

    # collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()


if __name__ == "__main__":
    # Check if the filter condition value is provided as a command line argument
    if len(sys.argv) < 2:
        print("Please provide the filter condition value as an argument.")
        sys.exit() 

    main(float(sys.argv[1]))
