def shuffle_df(df, num_workers):
    from pyspark.sql import functions as F
    df = df.withColumn('srand', F.rand())
    df = df.repartition(2 * num_workers, 'srand')
    print('shuffle df to partitions {}'.format(df.rdd.getNumPartitions()))
    df = df.sortWithinPartitions('srand')
    df = df.drop('srand')
    return df

def read_kudu(spark_session, url, column_name, sql=None, condition_select_conf='', shuffle=False, num_workers=1):
    from pyspark.sql import SQLContext
    from pyspark.sql import DataFrame

    queryCols=[]
    has_rand_column = False
    with open(column_name, 'r') as f_column_name:
        for line in f_column_name:
            line = line.split(" ")[-1].strip()
            if line == 'rand':
                has_rand_column = True
            queryCols.append(line)
    if not has_rand_column:
        print('append rand column by default')
        queryCols.append('rand')
    print('total cols from kudu: {}'.format(len(queryCols)))

    sc = spark_session.sparkContext
    ssqlContext = SQLContext(sc)._ssql_ctx
    jsparkSession = spark_session._jsparkSession
    if condition_select_conf == '':
        queryKudu = sc._jvm.com.mobvista.dataflow.apis.kuduUtils.QueryKudu.readKudu(jsparkSession, url, queryCols)
    else:
        print('use condition select conf: {}'.format(condition_select_conf))
        queryKudu = sc._jvm.com.mobvista.dataflow.apis.kuduUtils.QueryKudu.readKudu(jsparkSession, url, queryCols, condition_select_conf)
    kudu_df_tmp = DataFrame(queryKudu, ssqlContext)
    kudu_df_tmp.createOrReplaceTempView("kudu_df_tmp")
    if sql is not None:
        kudu_df = spark_session.sql(sql)
    else:
        kudu_df = spark_session.sql('select * from kudu_df_tmp')

    if shuffle and num_workers > 1:
        kudu_df = shuffle_df(kudu_df, num_workers)
    else:
        print("ignore shuffle")
    if not has_rand_column:
        kudu_df = kudu_df.drop('rand')
    return kudu_df

def read_s3_csv(spark_session, url, shuffle=False, num_workers=1,
                header=False, nullable=False, delimiter="\002", encoding="UTF-8"):
    from .url_utils import use_s3a
    df = (spark_session
             .read
             .format('csv')
             .option("header", str(bool(header)).lower())
             .option("nullable", str(bool(nullable)).lower())
             .option("delimiter", delimiter)
             .option("encoding", encoding)
             .load(use_s3a(url)))
    if shuffle and num_workers > 1:
        df = shuffle_df(df, num_workers)
    else:
        print("ignore shuffle")
    return df

def read_s3_image(spark_session, url):
    from .url_utils import use_s3a
    df = spark_session.read.format('image').option('dropInvalid', 'true').load(use_s3a(url))
    return df
