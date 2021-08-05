def redshift_query(url,Query):
    redshift_res = spark.read.format("jdbc").\
        option("driver", "com.amazon.redshift.jdbc42.Driver").\
        option("url", "jdbc:redshift://AWS location/dev").\
        option("query",Query).\
        option("user", "userName").\
        option("password","pswed").\
        load()
        #option("plugin_name", "com.amazon.redshift.plugin.AzureCredentialsProvider"). \
    return redshift_res

Query2 = "select * from schema.tableName"


df = redshift_query("url",Query2)
df.count()
