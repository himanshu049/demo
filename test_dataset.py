from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType,DoubleType,DateType
spark = SparkSession.builder.appName("SchemaValidation").master("local").getOrCreate()
enforced_schema =[{'name': 'EmpID', 'type': 'integer', 'nullable': False, 'metadata': {'iskey': False, 'sensitivity': '', 'alias_name': '', 'business_glossary': '', 'description': 'Your account ID number'}}]
# all_data1 = spark.read.format("csv").option('header', 'True').option('sep',',').option('multiline',True).option('escape', '\\').option('mergeSchema', 'true').schema(StructType.fromJson({"fields": enforced_schema})).load("s3://sbd-caspian-sandbox-landingzone/SFMC/BOUNCE/new/SFMC_BOUNCE_D_20210802083218_001.csv")
# all_data1 = spark.read.format("csv").option('header', 'True').option('sep',',').option('inferSchema','True').option('multiline',True).option('escape', '\\').option('mergeSchema', 'true').load("s3://test.csv")
all_data1 = spark.read.format("csv").option('header', 'True').option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").option('sep', ',').option('multiline',True).option('escape','\\' ).option('mergeSchema', 'true').schema(StructType.fromJson({"fields": enforced_schema})).load("s3://test.csv")
all_data1.show(1,truncate = False)

#all_data1.filter((all_data1.empkey == "123abcd") & (all_data1.ID == "123abcd")).toJSON().first()
all_data1.printSchema()
all_data1.dtypes
