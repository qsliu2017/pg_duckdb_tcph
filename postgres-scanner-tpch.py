import duckdb
import psycopg2
import tempfile
import time

DUCKDB_DATABASE_FILE = "tpch.duckdb"
POSTGRES_CONNECTION_STRING = "dbname=tpch"
# set a one-minute timeout for postgres to avoid waiting forever
POSTGRES_TIMEOUT_MS = 30000
POSTGRES_WORKERS = 10

# connect to databases
dcon = duckdb.connect(DUCKDB_DATABASE_FILE)
pcon = psycopg2.connect(POSTGRES_CONNECTION_STRING).cursor()
pcon.execute("ROLLBACK")  # ?!

# install and load DuckDB extensions required
dcon.execute("INSTALL tpch")
dcon.execute("LOAD tpch")

schemascript = """

CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152), PRIMARY KEY (N_NATIONKEY));

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152), PRIMARY KEY (R_REGIONKEY));

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL , PRIMARY KEY (P_PARTKEY));

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL, PRIMARY KEY (S_SUPPKEY));

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL , PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY));

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL, PRIMARY KEY (C_CUSTKEY));

CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL, PRIMARY KEY (O_ORDERKEY));

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL, PRIMARY KEY (L_ORDERKEY, L_LINENUMBER));

                             """


def lineitem_exists(con):
    try:
        con.execute("SELECT * FROM lineitem")
        return True
    except:
        return False


# create the tpch test data in duckdb if required
if not lineitem_exists(dcon):
    dcon.execute("CALL dbgen(sf=1)")

exportdir = ""
# create the tpch test data in postgres if required
if not lineitem_exists(pcon):
    exportdir = str(tempfile.mkdtemp())
    dcon.execute("EXPORT DATABASE '%s' (FORMAT CSV)" % exportdir)

    # FIXME do the actual load?
    pcon.execute(open("%s/schema.sql" % exportdir, "rb").read())
    pcon.execute(open("%s/load.sql" % exportdir, "rb").read())

    # some foreign keys for postgres to be fair
    pcon.execute(
        """

			ALTER TABLE PART
	  ADD CONSTRAINT part_kpey
		 PRIMARY KEY (P_PARTKEY);

	ALTER TABLE SUPPLIER
	  ADD CONSTRAINT supplier_pkey
		 PRIMARY KEY (S_SUPPKEY);

	ALTER TABLE PARTSUPP
	  ADD CONSTRAINT partsupp_pkey
		 PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY);

	ALTER TABLE CUSTOMER
	  ADD CONSTRAINT customer_pkey
		 PRIMARY KEY (C_CUSTKEY);

	ALTER TABLE ORDERS
	  ADD CONSTRAINT orders_pkey
		 PRIMARY KEY (O_ORDERKEY);

	ALTER TABLE LINEITEM
	  ADD CONSTRAINT lineitem_pkey
		 PRIMARY KEY (L_ORDERKEY, L_LINENUMBER);

	ALTER TABLE NATION
	  ADD CONSTRAINT nation_pkey
		 PRIMARY KEY (N_NATIONKEY);

	ALTER TABLE REGION
	  ADD CONSTRAINT region_pkey
		 PRIMARY KEY (R_REGIONKEY);

	   ALTER TABLE SUPPLIER
	ADD CONSTRAINT supplier_nation_fkey
	   FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY);

	   ALTER TABLE PARTSUPP
	ADD CONSTRAINT partsupp_part_fkey
	   FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY);
	   
	   ALTER TABLE PARTSUPP
	ADD CONSTRAINT partsupp_supplier_fkey
	   FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY);

	   ALTER TABLE CUSTOMER
	ADD CONSTRAINT customer_nation_fkey
	   FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY);

	   ALTER TABLE ORDERS
	ADD CONSTRAINT orders_customer_fkey
	   FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY);

	   ALTER TABLE LINEITEM
	ADD CONSTRAINT lineitem_orders_fkey
	   FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY);

	   ALTER TABLE LINEITEM
	ADD CONSTRAINT lineitem_partsupp_fkey
	   FOREIGN KEY (L_PARTKEY,L_SUPPKEY)
		REFERENCES PARTSUPP(PS_PARTKEY,PS_SUPPKEY);

	   ALTER TABLE NATION
	ADD CONSTRAINT nation_region_fkey
	   FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY);

		 """
    )

    pcon.execute("COMMIT; CHECKPOINT; ANALYZE;")

pcon.execute("SET statement_timeout = %d" % POSTGRES_TIMEOUT_MS)
pcon.execute("SET max_parallel_workers_per_gather = %d" % POSTGRES_WORKERS)
# pcon.execute('SET max_worker_processes = %d' % postgres_workers)
pcon.execute("SET max_parallel_workers = %d" % POSTGRES_WORKERS)

# duckdb + postgres scanner
dcon3 = duckdb.connect()
dcon3.execute("INSTALL postgres_scanner")
dcon3.execute("LOAD postgres_scanner")
dcon3.execute(
    "CALL postgres_attach('%s', filter_pushdown=true)" % POSTGRES_CONNECTION_STRING
)

# postgres + pg_duckdb
pcon4 = psycopg2.connect(POSTGRES_CONNECTION_STRING).cursor()
pcon4.execute("ROLLBACK")  # ?!
pcon4.execute("SET duckdb.execution = true")
pcon4.execute("SET statement_timeout = %d" % POSTGRES_TIMEOUT_MS)
pcon4.execute("SET max_parallel_workers_per_gather = %d" % POSTGRES_WORKERS)
# pcon4.execute('SET max_worker_processes = %d' % postgres_workers)
pcon4.execute("SET max_parallel_workers = %d" % POSTGRES_WORKERS)


def timeq(con, q):
    start = time.time()
    try:
        con.execute(q)
    except psycopg2.errors.QueryCanceled:
        con.execute("ROLLBACK")
        pcon.execute("SET statement_timeout = %d" % POSTGRES_TIMEOUT_MS)
        return "timeout"
    end = time.time()
    return round(end - start, 2)


print("experiment\tquery\ttime")

queries = dcon.execute("SELECT * FROM tpch_queries()").fetchall()

for i in range(5):
    for q in queries:
        print("duckdb\t%d\t%s" % (q[0], timeq(dcon, q[1])))

    for q in queries:
        print("postgres\t%d\t%s" % (q[0], timeq(pcon, q[1])))

    for q in queries:
        print("duckdb + postgres_scanner\t%d\t%s" % (q[0], timeq(dcon3, q[1])))

    for q in queries:
        print("postgres + pg_duckdb\t%d\t%s" % (q[0], timeq(pcon4, q[1])))
