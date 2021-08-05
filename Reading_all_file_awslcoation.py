df=spark.read.options(header=True,\
          inferSchema=True,\
          delimiter=",",\
          escape="\"",multiline = True,mergeSchema=True).csv("s3://location_files/*")
df.count()
