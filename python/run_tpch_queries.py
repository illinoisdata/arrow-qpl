import datetime
import polars as pl
# from polars_queries import utils
import pyarrow as pa
import utils
import time
from linetimer import CodeTimer, linetimer
import logging


def datetime_to_unix(dt):
    return int(time.mktime(dt.date().timetuple()))


logging.basicConfig(filename=f"./tpch_queries/queries.log",
                    filemode='w',
                    # format='[%(asctime)s | %(levelname)s]: %(message)s',
                    level=logging.INFO)

my_logger = logging.getLogger('TPCH')

def q1(Q_NUM):
    # var1 = datetime.datetime(1998, 9, 2)
    # var1 = int(time.mktime(datetime.date(1998, 9, 2).timetuple()))
    # var1 = datetime_to_unix(var1)
    var1 = 904712400

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        q = utils.get_line_item_ds()
    # q = q.with_column(pl.col("l_shipdate").str.strptime(pl.Datetime, fmt="%Y-%m-%d"))

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        q_final = (
            q.filter(pl.col("l_shipdate") <= var1)
            .group_by(["l_returnflag", "l_linestatus"])
            .agg(
                [
                    pl.sum("l_quantity").alias("sum_qty"),
                    pl.sum("l_extendedprice").alias("sum_base_price"),
                    (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                    .sum()
                    .alias("sum_disc_price"),
                    (
                        pl.col("l_extendedprice")
                        * (1.0 - pl.col("l_discount"))
                        * (1.0 + pl.col("l_tax"))
                    )
                    .sum()
                    .alias("sum_charge"),
                    pl.mean("l_quantity").alias("avg_qty"),
                    pl.mean("l_extendedprice").alias("avg_price"),
                    pl.mean("l_discount").alias("avg_disc"),
                    pl.count().alias("count_order"),
                ],
            )
            .sort(["l_returnflag", "l_linestatus"])
        )
    
    
    # print(q_final.head(10))

    # utils.run_query(Q_NUM, q_final)
    return q_final


def q2(Q_NUM):

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        region_ds = utils.get_region_ds()
        nation_ds = utils.get_nation_ds()
        supplier_ds = utils.get_supplier_ds()
        part_ds = utils.get_part_ds()
        part_supp_ds = utils.get_part_supp_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        var1 = 15
        var2 = "BRASS"
        var3 = "EUROPE"

        final_cols = [
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ]
        result_q1 = (
            part_ds.join(part_supp_ds, left_on="p_partkey", right_on="ps_partkey")
            .join(supplier_ds, left_on="ps_suppkey", right_on="s_suppkey")
            .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
            .join(region_ds, left_on="n_regionkey", right_on="r_regionkey")
            .filter(pl.col("p_size") == var1)
            .filter(pl.col("p_type").str.ends_with(var2))
            .filter(pl.col("r_name") == var3)
        )

        result2 = (result_q1.group_by("p_partkey")
            .agg(pl.min("ps_supplycost").alias("ps_supplycost_min")))

        q_final = (
            result_q1.join(
                result2,
                left_on=["p_partkey", "ps_supplycost"],
                right_on=["p_partkey", "ps_supplycost_min"]
            )
            .select(final_cols)
            .sort(
                by=["s_acctbal", "n_name", "s_name", "p_partkey"],
                descending=[True, False, False, False],
            )
            .limit(100)
            .with_columns(pl.col(pl.datatypes.Utf8).str.strip_chars().keep_name())
        )

    # print(q_final.head(10))
#     utils.run_query(Q_NUM, q_final)
    return q_final


def q3(Q_NUM):
    # var1 = var2 = datetime(1995, 3, 15)
    # var1 = var2 = int(time.mktime(datetime.date(1998, 3, 15).timetuple()))

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        customer_ds = utils.get_customer_ds()
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var1 = 889941600
        var2 = 889941600
        var3 = "BUILDING"

        q_final = (
            customer_ds.filter(pl.col("c_mktsegment") == var3)
            .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .filter(pl.col("o_orderdate") < var2)
            .filter(pl.col("l_shipdate") > var1)
            .with_columns(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
            )
            .group_by(["o_orderkey", "o_orderdate", "o_shippriority"])
            .agg([pl.sum("revenue")])
            .select(
                [
                    pl.col("o_orderkey").alias("l_orderkey"),
                    "revenue",
                    "o_orderdate",
                    "o_shippriority",
                ]
            )
            .sort(by=["revenue", "o_orderdate"], descending=[True, False])
        )

    # print(q_final.head(10))
    # utils.run_query(Q_NUM, q_final)
    return q_final

def q4(Q_NUM):
    # var1 = datetime.datetime(1993, 7, 1)
    # var2 = datetime.datetime(1993, 10, 1)
    # var1 = datetime_to_unix(var1)
    # var2 = datetime_to_unix(var2)

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var1 = 741502800
        var2 = 749451600

        q_final = (
            line_item_ds.join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
            .filter(pl.col("o_orderdate") >= var1)
            .filter(pl.col("o_orderdate") < var2)
            .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
            .unique(subset=["o_orderpriority", "l_orderkey"])
            .group_by("o_orderpriority")
            .agg(pl.count().alias("order_count"))
            .sort(by="o_orderpriority")
            .with_columns(pl.col("order_count").cast(pl.datatypes.Int64))
        )

    # print(q_final.head(10))
    return q_final


def q5(Q_NUM):


    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        region_ds = utils.get_region_ds()
        nation_ds = utils.get_nation_ds()
        customer_ds = utils.get_customer_ds()
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()
        supplier_ds = utils.get_supplier_ds()
    
    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var1 = "ASIA"
        # var2 = datetime.datetime(1994, 1, 1)
        # var3 = datetime.datetime(1995, 1, 1)
        # var2 = datetime_to_unix(var2)
        # var3 = datetime_to_unix(var3)
        var2 = 757404000
        var3 = 788940000
        q_final = (
            region_ds.join(nation_ds, left_on="r_regionkey", right_on="n_regionkey")
            .join(customer_ds, left_on="n_nationkey", right_on="c_nationkey")
            .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .join(
                supplier_ds,
                left_on=["l_suppkey", "n_nationkey"],
                right_on=["s_suppkey", "s_nationkey"],
            )
            .filter(pl.col("r_name") == var1)
            .filter(pl.col("o_orderdate") >= var2)
            .filter(pl.col("o_orderdate") < var3)
            .with_columns(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
            )
            .group_by("n_name")
            .agg([pl.sum("revenue")])
            .sort(by="revenue", descending=True)
        )

    # print(q_final.head(10))
    return q_final


def q6(Q_NUM):
    # var1 = datetime.datetime(1994, 1, 1)
    # var2 = datetime.datetime(1995, 1, 1)
    # var1 = datetime_to_unix(var1)
    # var2 = datetime_to_unix(var2)
    # print(var1, var2)

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        line_item_ds = utils.get_line_item_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        var1 = 757404000
        var2 = 788940000
        var3 = 24
        q_final = (
            line_item_ds.filter(pl.col("l_shipdate") >= var1)
            .filter(pl.col("l_shipdate") < var2)
            .filter((pl.col("l_discount") >= 0.05) & (pl.col("l_discount") <= 0.07))
            .filter(pl.col("l_quantity") < var3)
            .with_columns(
                (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
            )
            .select(pl.sum("revenue").alias("revenue"))
        )

    # print(q_final.head(10))
    return q_final


def q7(Q_NUM):
    
    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        nation_ds = utils.get_nation_ds()
        customer_ds = utils.get_customer_ds()
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()
        supplier_ds = utils.get_supplier_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        n1 = nation_ds.filter(pl.col("n_name") == "FRANCE")
        n2 = nation_ds.filter(pl.col("n_name") == "GERMANY")

        df1 = (
            customer_ds.join(n1, left_on="c_nationkey", right_on="n_nationkey")
            .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .rename({"n_name": "cust_nation"})
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .join(n2, left_on="s_nationkey", right_on="n_nationkey")
            .rename({"n_name": "supp_nation"})
        )

        df2 = (
            customer_ds.join(n2, left_on="c_nationkey", right_on="n_nationkey")
            .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .rename({"n_name": "cust_nation"})
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .join(n1, left_on="s_nationkey", right_on="n_nationkey")
            .rename({"n_name": "supp_nation"})
        )

        # var1 = datetime.datetime(1995, 1, 1)
        # print(datetime_to_unix(var1))
        # var1 = datetime_to_unix(var1)
        var1 = 788940000

        # var2 = datetime.datetime(1996, 12, 31)
        # print(datetime_to_unix(var2))
        # var2 = datetime_to_unix(var2)
        var2 = 852012000

        q_final = (
            pl.concat([df1, df2])
            .filter(pl.col("l_shipdate") >= var1)
            .filter(pl.col("l_shipdate") <= var2)
            .with_columns(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume")
            )
            # .with_columns(pl.col("l_shipdate").dt.year().alias("l_year"))
            .with_columns(pl.from_epoch(pl.col("l_shipdate"), time_unit='s').dt.year().alias("l_year")) # TODO: Fix this
            .group_by(["supp_nation", "cust_nation", "l_year"])
            .agg([pl.sum("volume").alias("revenue")])
            .sort(by=["supp_nation", "cust_nation", "l_year"])
        )

    # print(q_final.head(10))
    return q_final


def q8(Q_NUM):
    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        nation_ds = utils.get_nation_ds()
        customer_ds = utils.get_customer_ds()
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()
        supplier_ds = utils.get_supplier_ds()
        part_ds = utils.get_part_ds()
        region_ds = utils.get_region_ds()

    # Query Variables
    # var_date_start = datetime.datetime(1995,1,1)
    # var_date_end = datetime.datetime(1996,12,31)
    # var_date_start = datetime_to_unix(var_date_start)
    # var_date_end = datetime_to_unix(var_date_end)
    # print(var_date_end, var_date_start)
    
    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        var_date_start = 788940000
        var_date_end = 852012000
        var_r_name = 'AMERICA'
        var_s_nation = 'BRAZIL'
        var_p_type = 'ECONOMY ANODIZED STEEL'

        q_final = (
            line_item_ds
            .join(
                orders_ds.filter((pl.col("o_orderdate") >= var_date_start) & (pl.col("o_orderdate") <= var_date_end)),
                left_on = "l_orderkey",
                right_on = "o_orderkey"
            )
            .join(
                part_ds.filter(pl.col("p_type") == var_p_type),
                left_on = "l_partkey",
                right_on = "p_partkey"
            )
            .join(
                customer_ds,
                left_on = "o_custkey",
                right_on = "c_custkey"
            )
            .join(
                nation_ds.join(region_ds.filter(pl.col("r_name") == var_r_name), left_on = "n_regionkey", right_on = "r_regionkey"),
                left_on = "c_nationkey",
                right_on = "n_nationkey"
            )
            .join(
                supplier_ds.join(nation_ds, left_on = "s_nationkey", right_on = "n_nationkey").rename({"n_name": "n2.n_name"}),
                left_on = "l_suppkey",
                right_on = "s_suppkey"
            )
            .with_columns([
                pl.from_epoch(pl.col("o_orderdate"), time_unit='s').dt.year().alias("o_year"),
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume"),
                pl.col("n2.n_name").alias("nation")
            ])
            .group_by(["o_year"])
            .agg([
                ((pl.col("volume") * (pl.col("n2.n_name") == var_s_nation)).sum()/pl.col("volume").sum()).alias("mkt_share")
            ])
            .sort(["o_year"])
        )

    # print(q_final.head(10))
    return q_final


def q9(Q_NUM):

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):

        part_ds = utils.get_part_ds()
        supplier_ds = utils.get_supplier_ds()
        line_item_ds = utils.get_line_item_ds()
        part_supp_ds = utils.get_part_supp_ds()
        orders_ds = utils.get_orders_ds()
        nation_ds = utils.get_nation_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var_color = 'green' #LIKE GREEN
        final_cols = [
            "nation",
            "o_year",
            "sum_profit",
        ]
        q_final = (
            line_item_ds.join(part_supp_ds, left_on=["l_suppkey", "l_partkey"], right_on=["ps_suppkey", "ps_partkey"])
            .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .join(nation_ds, left_on="s_nationkey", right_on="n_nationkey")
            .join(part_ds, left_on="l_partkey", right_on="p_partkey")
            .join(orders_ds, left_on="l_orderkey", right_on="o_orderkey")
            .filter(pl.col("p_name").str.contains(var_color))
            .rename({"n_name": "nation"})
            .with_columns(pl.from_epoch(pl.col("o_orderdate"), time_unit='s').dt.year().alias("o_year")) #extract year from orderdate
            .with_columns(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount")) - pl.col("ps_supplycost") * pl.col("l_quantity")).alias("amount")
                )
            .group_by(["nation", "o_year"])
            .agg([pl.sum("amount").alias("sum_profit")])
            .select(final_cols)
            .sort(by=["nation", "o_year"], descending=[False, True])
        )

    # print(q_final.head(10))
    return q_final


def q10(Q_NUM):
    # var1 = datetime.datetime(1993, 10, 1)
    # var2 = datetime.datetime(1994, 1, 1)
    # var1 = datetime_to_unix(var1)
    # var2 = datetime_to_unix(var2)

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        customer_ds = utils.get_customer_ds()
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()
        nation_ds = utils.get_nation_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var1 = 749451600
        var2 = 757404000
        var3 = "R"
        
        q_final = (
            customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .join(nation_ds, left_on="c_nationkey", right_on="n_nationkey")
            .filter(pl.col("o_orderdate") >= var1)
            .filter(pl.col("o_orderdate") < var2)
            .filter(pl.col("l_returnflag") == var3)
            .with_columns(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
            )
            .group_by(["c_custkey","c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"])
            .agg([pl.sum("revenue")])
            .select(
                [
                    "c_custkey",
                    "c_name",
                    "revenue",
                    "c_acctbal",
                    "n_name",
                    "c_address",
                    "c_phone",
                    "c_comment"
                ]
            )
            .sort(by="revenue", descending=True)
            .with_columns(pl.col(pl.datatypes.Utf8).str.strip_chars().keep_name())
        )

    # print(q_final.head(10))
    return q_final


def q11(Q_NUM):

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):

        partsupp_ds = utils.get_part_supp_ds()
        supplier_ds = utils.get_supplier_ds()
        nation_ds = utils.get_nation_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var_n_name = "GERMANY"

        ps_supplycost_agg = (
            partsupp_ds
            .join(
                supplier_ds.join(
                    nation_ds.filter(pl.col("n_name") == var_n_name),
                    left_on = "s_nationkey",
                    right_on = "n_nationkey"
                ),
                left_on = "ps_suppkey",
                right_on = "s_suppkey"
            )
            .with_columns((pl.col("ps_supplycost") * pl.col("ps_availqty") * 0.0001).alias("value_limit"))
            .select(["value_limit"])
            .sum()
        )

        q_final = (
            partsupp_ds
            .join(
                supplier_ds.join(
                    nation_ds.filter(pl.col("n_name") == var_n_name),
                    left_on = "s_nationkey",
                    right_on = "n_nationkey"
                ),
                left_on = "ps_suppkey",
                right_on = "s_suppkey"
            )
            .group_by(["ps_partkey"])
            .agg([
                (pl.col("ps_supplycost") * pl.col("ps_availqty")).sum().alias("value")
            ])
            .filter(pl.col("value") > pl.lit(ps_supplycost_agg.get_column("value_limit")))
            .sort(["value"], descending=[True])
        )

    # print(q_final.head(10))
    return q_final

def q12(Q_NUM):

    # var_date = datetime.datetime(1994, 1, 1)
    # var_date_interval_1yr = datetime.datetime(1995, 1, 1)
    # var_date = datetime_to_unix(var_date)
    # var_date_interval_1yr = datetime_to_unix(var_date_interval_1yr)
    var_date = 757404000
    var_date_interval_1yr = 788940000

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()
    
    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        var_ship_mode1 = 'MAIL'
        var_ship_mode2 = 'SHIP'

        q_final = (
            orders_ds.join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .filter((pl.col("l_shipmode") == var_ship_mode1) | (pl.col("l_shipmode") == var_ship_mode2))
            .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
            .filter(pl.col("l_shipdate") < pl.col("l_commitdate"))
            .filter(pl.col("l_receiptdate") >= var_date)
            .filter(pl.col("l_receiptdate") < var_date_interval_1yr)
            .group_by(["l_shipmode"])
            .agg(
                [
                    ((pl.col("o_orderpriority") == "1-URGENT") | (pl.col("o_orderpriority") == "2-HIGH")).sum().alias("high_line_count"),
                    ((pl.col("o_orderpriority") != "1-URGENT") & (pl.col("o_orderpriority") != "2-HIGH")).sum().alias("low_line_count")
                ]
            )
            .sort(by="l_shipmode")
        )

    # print(q_final.head(10))
    return q_final

def q13(Q_NUM):
    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        orders_ds = utils.get_orders_ds()
        customer_ds = utils.get_customer_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        q_final = (
            customer_ds
            .join(
                orders_ds.filter(~pl.col("o_comment").str.contains("(.*)special(.*)requests(.*)")),
                left_on = "c_custkey",
                right_on = "o_custkey",
                how = "left"
            )
            .group_by(["c_custkey"])
            .agg(pl.col("o_orderkey").drop_nulls().count().alias("c_count"))
            .group_by(["c_count"])
            .agg(pl.count().alias("custdist"))
            .sort(["custdist","c_count"], descending=[True, True])
        )

    # print(q_final.head(10))
    return q_final

def q14(Q_NUM):

    # VAR1 = datetime.datetime(1995, 9, 1)
    # VAR2 = datetime.datetime(1995, 10, 1)
    # VAR1 = datetime_to_unix(VAR1)
    # VAR2 = datetime_to_unix(VAR2)
    # print(VAR1, VAR2)

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        line_item_ds = utils.get_line_item_ds()
        part_ds = utils.get_part_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        VAR1 = 809931600
        VAR2 = 812523600
        q_final = (
            line_item_ds
            .filter((pl.col("l_shipdate") >= VAR1) & (pl.col("l_shipdate") < VAR2))
            .join(part_ds, left_on="l_partkey", right_on="p_partkey")
            .with_columns(
                [
                    pl.col("p_type").str.starts_with("PROMO").alias("is_promo"),
                    (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue"),
                ])
            .with_columns([
                (pl.col("revenue") * pl.col("is_promo")).alias("num_revenue"),
                pl.col("revenue").alias("den_revenue"),
            ])
            .sum()
            .select(
                (100.0 * pl.col("num_revenue")/pl.col("den_revenue")).alias("promo_revenue")
            )
        )

    # print(q_final.head(10))
    return q_final

def q15(Q_NUM):
    # var_date = datetime.datetime(1996, 1, 1)
    # var_date_interval_3mon = datetime.datetime(1996, 4, 1)
    # var_date = datetime_to_unix(var_date)
    # var_date_interval_3mon = datetime_to_unix(var_date_interval_3mon)

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        supplier_ds = utils.get_supplier_ds()
        line_item_ds = utils.get_line_item_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var_date = 820476000
        var_date_interval_3mon = 828338400

        final_cols = [
            "s_suppkey",
            "s_name",
            "s_address",
            "s_phone",
            "total_revenue"
        ]

        revenue0 = (
            line_item_ds
            .filter((pl.col("l_shipdate") >= var_date) & (pl.col("l_shipdate") < var_date_interval_3mon))
            .with_columns((pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue"))
            .group_by("l_suppkey")
            .agg(pl.sum("revenue").alias('total_revenue'))
            .select(["l_suppkey", "total_revenue"])
            .rename({"l_suppkey": "supplier_no"})
        )

        max_total_revenue = revenue0.max().get_column("total_revenue")

        q_final = (
            supplier_ds
            .join(revenue0.filter(pl.col("total_revenue") == max_total_revenue), left_on="s_suppkey", right_on="supplier_no")
            .select(final_cols)
            .sort("s_suppkey")
        )

    # print(q_final.head(10))
    return q_final

def q16(Q_NUM):

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        part_ds = utils.get_part_ds()
        part_supp_ds = utils.get_part_supp_ds()
        supp_ds = utils.get_supplier_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        supp_ds_filter = (
            supp_ds
            .filter(pl.col("s_comment").str.contains("(.*)Customer(.*)Complaints(.*)"))
            .select("s_suppkey")
        )

        part_ds_filter = (
            part_ds
            .filter((pl.col("p_brand") != "Brand#45") &
                (~pl.col("p_type").str.starts_with("MEDIUM POLISHED")) &
                (pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
            )
        )

        q_final = (
            part_supp_ds
            .filter(~pl.col("ps_suppkey").is_in(pl.lit(supp_ds_filter.get_column("s_suppkey"))))
            .join(part_ds_filter, left_on="ps_partkey", right_on="p_partkey")
            .group_by(["p_brand","p_type","p_size"])
            .agg(pl.col("ps_suppkey").n_unique().alias("supplier_cnt"))
            .sort(["supplier_cnt","p_brand","p_type","p_size"], descending=[True,False,False,False])
        )

    # print(q_final.head(10))
    return q_final

def q17(Q_NUM):
    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        lineitem_ds = utils.get_line_item_ds()
        part_ds = utils.get_part_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        lineitem_grouped = (
            lineitem_ds
            .group_by(["l_partkey"])
            .agg(pl.col("l_quantity").mean().alias("l_quantity_avg"))
        )

        q_final = (
            part_ds
            .filter((pl.col("p_brand") == "Brand#23") & (pl.col("p_container") == "MED BOX"))
            .join(lineitem_ds, left_on="p_partkey", right_on="l_partkey")
            .join(lineitem_grouped, left_on="p_partkey", right_on="l_partkey")
            .filter(pl.col("l_quantity") < 0.2 * pl.col("l_quantity_avg"))
            .sum()
            .with_columns((pl.col("l_extendedprice")/7.0).alias("avg_yearly"))
            .select(pl.col("avg_yearly"))
        )

    # print(q_final.head(10))
    return q_final

def q18(Q_NUM):

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()
        customer_ds = utils.get_customer_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        var_quantity = 300

        final_cols = [
            "c_name",
            "c_custkey",
            "o_orderkey",
            "o_orderdate",
            "o_totalprice",
            "sum"
        ]
        filtered_line_item_ds = (line_item_ds.group_by("l_orderkey")
            .agg(pl.sum("l_quantity").alias("sum_l_quantity"))
            .select(["l_orderkey", "sum_l_quantity"])
            .filter(pl.col("sum_l_quantity") > var_quantity)
            .with_columns(pl.col("sum_l_quantity").cast(pl.datatypes.Float64).alias("sum")))

        q_final = (
            customer_ds.join(orders_ds, left_on="c_custkey", right_on="o_custkey")
            .join(filtered_line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
            .select(final_cols)
            .sort(["o_totalprice", "o_orderdate"], descending=[True, False])
        )

    # print(q_final.head(10))
    return q_final

def q19(Q_NUM):

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        line_item_ds = utils.get_line_item_ds()
        part_ds = utils.get_part_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        cols = [
            "l_shipmode",
            "l_shipinstruct",
            "p_brand",
            "p_container",
            "l_quantity",
            "p_size"
        ]

        q_final = (
            line_item_ds
            .filter((pl.col("l_shipmode").is_in(pl.lit(pl.Series(["AIR","AIR REG"])))) & (pl.col("l_shipinstruct") == "DELIVER IN PERSON"))
            .join(part_ds, left_on="l_partkey", right_on="p_partkey")
            .filter(
                (
                    (pl.col("p_brand") == "Brand#12") &
                    (pl.col("p_container").is_in(pl.lit(pl.Series(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])))) &
                    (pl.col("l_quantity").is_between(1,11,closed='both')) &
                    (pl.col("p_size").is_between(1,5,closed='both')) &
                    (pl.col("l_shipmode").is_in(pl.lit(pl.Series(["AIR","AIR REG"])))) &
                    (pl.col("l_shipinstruct") == "DELIVER IN PERSON")
                ) |
                (
                    (pl.col("p_brand") == "Brand#23") &
                    (pl.col("p_container").is_in(pl.lit(pl.Series(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])))) &
                    (pl.col("l_quantity").is_between(10,20,closed='both')) &
                    (pl.col("p_size").is_between(1,10,closed='both'))
                ) |
                (
                    (pl.col("p_brand") == "Brand#34") &
                    (pl.col("p_container").is_in(pl.lit(pl.Series(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])))) &
                    (pl.col("l_quantity").is_between(20,30,closed='both')) &
                    (pl.col("p_size").is_between(1,15,closed='both'))
                )
            )
            .with_columns(
                (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
            ).select("revenue").sum()
        )

    # print(q_final.head(10))
    return q_final


def q20(Q_NUM):
    # VAR1 = datetime.datetime(1994, 1, 1)
    # VAR2 = datetime.datetime(1995, 1, 1)
    # VAR1 = datetime_to_unix(VAR1)
    # VAR2 = datetime_to_unix(VAR2)


    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        line_item_ds = utils.get_line_item_ds()
        part_ds = utils.get_part_ds()
        part_supp_ds = utils.get_part_supp_ds()
        nation_ds = utils.get_nation_ds()
        supp_ds = utils.get_supplier_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):

        VAR1 = 757404000
        VAR2 = 788940000

        part_subquery = (
            part_ds
            .filter(pl.col("p_name").str.starts_with("forest"))
            .select("p_partkey")
        )

        lineitem_group_ds = (
            line_item_ds
            .filter((pl.col("l_shipdate") >= VAR1) & (pl.col("l_shipdate") < VAR2))
            .group_by(["l_partkey","l_suppkey"])
            .agg((0.5 * pl.col("l_quantity").sum()).alias("l_quantity_sum"))
        )

        ps_suppkey_subquery = (
            part_supp_ds
            .join(part_subquery, left_on="ps_partkey", right_on="p_partkey")
            .join(lineitem_group_ds, left_on=["ps_partkey","ps_suppkey"], right_on=["l_partkey","l_suppkey"])
            .filter(pl.col("ps_availqty") > pl.col("l_quantity_sum").floor())
            .select("ps_suppkey")
        )

        q_final = (
            supp_ds
            .join(nation_ds.filter(pl.col("n_name") == "CANADA"), left_on="s_nationkey", right_on="n_nationkey")
            ## Alternate possibility: Instead of doing a collect(), perform a unique and then JOIN on ps_suppkey.
            # .join(ps_suppkey_subquery, left_on="s_suppkey", right_on="ps_suppkey")
            .filter(pl.col("s_suppkey").is_in(pl.lit(ps_suppkey_subquery.get_column("ps_suppkey"))))
            .select(["s_name","s_address"])
            .sort("s_name")
        )

    # print(q_final.head(10))
    return q_final


def q21(Q_NUM):

    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        supplier_ds = utils.get_supplier_ds()
        line_item_ds = utils.get_line_item_ds()
        orders_ds = utils.get_orders_ds()
        nation_ds = utils.get_nation_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        var_nation = 'SAUDI ARABIA'

        line_item_order_ds = (
            line_item_ds
            .join(orders_ds.filter(pl.col("o_orderstatus") == "F"), left_on="l_orderkey", right_on="o_orderkey")
        )

        line_item_order_faulty = (
            line_item_order_ds
            .filter(pl.col("l_receiptdate") > pl.col("l_commitdate"))
        )

        num_total_suppliers = (
            line_item_order_ds
            .group_by(["l_orderkey"])
            .agg([
                pl.col("l_suppkey").n_unique().alias("num_total_suppliers"),
            ])
            .filter(pl.col("num_total_suppliers") >= 2)
        )
        num_faulty_suppliers = (
            line_item_order_faulty
            .group_by(["l_orderkey"])
            .agg([
                pl.col("l_suppkey").n_unique().alias("num_faulty_supplier")
            ])
            .filter(pl.col("num_faulty_supplier") == 1)
        )

        supplier_nation_ds = (
            supplier_ds
            .join(
                nation_ds.filter(pl.col("n_name") == var_nation),
                left_on="s_nationkey",
                right_on="n_nationkey"
            )
        )

        q_final = (
            line_item_order_faulty
            .join(supplier_nation_ds, left_on="l_suppkey", right_on="s_suppkey")
            .group_by(["l_orderkey","l_suppkey"])
            .agg(pl.count().alias("l_lineitem_count"))
            .join(num_total_suppliers, left_on="l_orderkey", right_on="l_orderkey")
            .join(num_faulty_suppliers, left_on="l_orderkey", right_on="l_orderkey")
            .join(supplier_ds, left_on="l_suppkey", right_on="s_suppkey")
            .group_by(["s_name"])
            .agg(pl.col("l_lineitem_count").sum().alias("numwait"))
            .sort(["numwait", "s_name"], descending=[True, False])
        )

    # print(q_final.head(10)) 
    return q_final


def q22(Q_NUM):
    with CodeTimer(f"Q{Q_NUM} - Read Time", unit='s', logger_func=my_logger.info):
        customer_ds = utils.get_customer_ds()
        orders_ds = utils.get_orders_ds()

    with CodeTimer(f"Q{Q_NUM} - Query Time", unit='s', logger_func=my_logger.info):
        var_list = [13, 31, 23, 29, 30, 18, 17]

        avg_c_acctbal = (
            customer_ds
            .filter((pl.col("c_acctbal") > 0) & (pl.col("c_phone").str.slice(0,2).cast(pl.Int32).is_in(pl.lit(pl.Series(var_list)))))
            .select(pl.col("c_acctbal"))
            .mean()
        )

        q_final = (
            customer_ds
            .with_columns((pl.col("c_phone").str.slice(0,2).alias("cntrycode").cast(pl.Int32)))
            .filter(pl.col("cntrycode").is_in(pl.lit(pl.Series(var_list))))
            .filter(pl.col("c_acctbal") > pl.lit(avg_c_acctbal.get_column("c_acctbal")))
            .join(orders_ds, left_on=["c_custkey"], right_on=["o_custkey"],how="left")
            .filter(pl.col("o_orderkey").is_null())
            .group_by(["cntrycode"])
            .agg([
                pl.count().alias("numcust"),
                pl.col("c_acctbal").sum().alias("totacctbal")
            ]).sort("cntrycode")
        )

    # print(q_final.head(10))
    return q_final


if __name__ == "__main__":

    for Q_NUM in range(1, 23):
        function_name = 'q' + str(Q_NUM)
        function = globals()[function_name]
        q_res = function(Q_NUM)

        with CodeTimer(f"Q{Q_NUM} - Write Time", unit='s', logger_func=my_logger.info):
            utils.save_query_result(q_res, Q_NUM=Q_NUM)

    # q1()