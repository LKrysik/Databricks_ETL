# Databricks notebook source
# MAGIC %md ##Data Reader

# COMMAND ----------

class data_reader:
  def __init__(self, file_location, file_type ,**param):

    self.file_type             = file_type
    self.file_location         = file_location
    self.schema_dict           = param['schema_dict']            if 'schema_dict'            in param else None
    self.data_address          = param['data_address']           if 'data_address'           in param else None    #XLSX
    self.infer_schema          = param['infer_schema']           if 'infer_schema'           in param else 'true'
    self.header                = param['header']                 if 'header'                 in param else 'true'
    self.delimiter             = param['delimiter']              if 'delimiter'              in param else ','     #CSV
    self.nulls                 = param['nulls']                  if 'nulls'                  in param else 'true'  #treatEmptyValuesAsNulls
    self.use_schema            = param['use_schema']             if 'use_schema'             in param else True    #Use schema during data read. If false only rename and cast columns 
    self.custom_cast           = param['custom_cast']            if 'custom_cast'            in param else False   #Cast columns after data read
    self.select_columns        = param['select_columns']         if 'select_columns'         in param else False   #Select columns for output
    self.standardize           = param['standardize']            if 'standardize'            in param else False   #standardize columns names
    
    #define empty dataframe
    self.df = None
    #define schema
    self.schema = self.schema_dict_to_schema() if self.use_schema else None
    #read data
    self.read()
    
    
  def schema_dict_to_schema(self) -> StructType():
      if self.schema_dict:
        return StructType([StructField(key, value[1], value[2]) for key, value in  self.schema_dict.items()]) 

  def standardize_columns(self):
    for col in self.df.columns:
      self.df  = (self.df 
                  .withColumnRenamed(col,
                                     col
                                     .replace(" ", "")
                                     .replace("_", "")
                                     .replace("/", "")
                                     .replace("(", "")
                                     .replace(")", "")
                                     .replace("-", "")
                                     .replace('\r','')
                                     .replace('\n','')
                                     .replace(',','')  
                                     .replace('.','')  
                                     .replace('`','')))
  
  def read(self):
    
    if self.file_type == 'xlsx':
      self.df = spark.read.format("com.crealytics.spark.excel")\
        .option("inferSchema", self.infer_schema) \
        .option("dataAddress", self.data_address) \
        .option("treatEmptyValuesAsNulls", self.nulls) \
        .option("header", self.header) \
        .option("maxRowsInMemory",20) \
        .load(self.file_location,schema=self.schema)  

      
    if self.file_type == 'csv':
      self.df = spark.read.format('csv') \
        .option("inferSchema", self.infer_schema) \
        .option("header", self.header) \
        .option("sep", self.delimiter) \
        .load(self.file_location, schema=self.schema)

    if self.schema_dict:
      print('fi schema')
      #Schema columns that exists in source data
      columns = [(key,value) for key, value in self.schema_dict.items() if key in self.df.columns]  
      not_exists_columns = [(key,value) for key, value in self.schema_dict.items() if key not in self.df.columns]  
      if not_exists_columns:
        print("Following columns do not exists in data source - checck your schema")
        print(not_exists_columns)
        
      #Cast columns after data read
      if self.custom_cast:
        for key, value in columns:
          if value[3]:
            self.df = self.df.withColumn(key, col('`'+key+'`').cast(value[1]))
        
      #Select columns for output   
      if self.select_columns: #select choosen columns
        self.df = self.df.select(*[col('`'+key+'`') for key, value in columns if value[4]])  

      #Rename columns
      if self.schema_dict:      
        for key, value in columns:
          self.df = self.df.withColumnRenamed(key, value[0])

    if self.standardize:
      self.standardize_columns()
      
  def return_df(self):
    return self.df
  
  

# COMMAND ----------

###SAMPLE USAGE
#
#file_location      = "/mnt/RAW/FILES/SomeFile.xlsx"
#
#SCHEMA_DICT = {
#              #Source column name              #Rename to                      #Type for schema            Nullable column     cast if custom_cast is True      select column if select_columns is True
                                                                                                                            #after data read (cast to value[0])
#              #key                            : (value[0]                     ,value[1]                   ,value[2]           ,value[3]                        ,value[4])

#              'Key'                           : ('ID'                         , Types.IntegerType()       , True              , True                           , True),
#              'Name'                          : ('Name'                       , Types.StringType()        , True              , True                           , True),
#              'EmpID'                         : ('EmployeeID'                 , Types.IntegerType()       , True              , True                           , False),
#              'CategoryName'                  : ('CategoryName'               , Types.StringType()        , True              , False                          , True)
#
#              }
#
#

#Read data with schema
#DF_input = (data_reader(file_location = file_location, file_type = 'xlsx', data_address = "'Sheet1'!A1", schema_dict = SCHEMA_DICT)
#Read data with schema and select columns to output select_columns=True
#DF_input = (data_reader(file_location = file_location, file_type = 'xlsx', data_address = "'Sheet1'!A1", schema_dict = SCHEMA_DICT)
#Do not apply schema during data read (use_schema = False), use it to select columns and cast columns types after data read (custom_cast=True)
#DF_input = (data_reader(file_location = file_location, file_type = 'xlsx', data_address = "'Sheet1'!A1", infer_schema= 'false', first_row_is_header = 'true', schema_dict = SCHEMA_DICT, use_schema = False, select_columns = True, custom_cast =T rue).return_df())
#Read file without schema (as is)
#DF_input = (data_reader(file_location = file_location, file_type = 'xlsx', data_address = "'Sheet1'!A1") 
#Read file without schema (as is) and with standardize columns names (ex. remove dots, dashes, ...)
#DF_input = (data_reader(file_location = file_location, file_type = 'xlsx', data_address = "'Sheet1'!A1", standardize = True)

#
#display(DF_input)

# COMMAND ----------


