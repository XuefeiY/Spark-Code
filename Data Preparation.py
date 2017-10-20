# =============================================================================
# # Import Package
# =============================================================================
import os
import pandas as pd
import numpy as np
import pyspark.sql.context
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import isnull, when, count, col, size, udf, explode
import pyspark.sql.functions as F
import keras
from keras.models import Sequential
from keras.layers import LSTM
from keras.layers import Dense
from hyperopt import fmin, tpe, hp, STATUS_OK, Trials


# =============================================================================
# # Load data
# =============================================================================
# hided


# =============================================================================
# Truncate data
# =============================================================================
# Classify columns
# hided


# Filter Data
df = df.filter(df.count_total >= 14)

# Remove & Look back
## Define Function
def window_func(x, sales_col, visits_col, id_col, steps):
    try:
       d = x.asDict()
    except:
       d = x    

    for col1 in sales_col:
        d[col1].pop(0)
        tmp = d[col1]
        d[col1] = []
        for i in range(len(tmp) - steps + 1):
            d[col1].append(tmp[i:(i+steps)])

    for col2 in visits_col:
        d[col2].pop(0)
        d[col2].pop()
        tmp = d[col2]
        d[col2] = []
        for i in range(len(tmp) - steps + 1):
            d[col2].append(tmp[i:(i+steps)])   
    try:
    	N = len(d[sales_col[0]])
    except:
    	N = None

    for col3 in id_col:
    	if N:
    		d[col3] = [d[col3]]*N 
    return d

## Execute function
rdd = df.rdd.map(lambda x: window_func(x, sales_col, visits_col, id_col, 13))
df_new = spark.createDataFrame(rdd)


## Explode rows
# Method 1:
#rdd_new = df_new.rdd.flatMap(lambda x: [(x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28) for x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28 in zip(*x)])

# Method 2:
def explode_lol(x):
    try:
       d = x.asDict()
    except:
       d = x
    keys = d.keys()
    for i in range(len(d[keys[0]])):
       d_new = {}
       d_new = {p:d[p][i] for p in keys}
       yield d_new
       
 # Method 3:      
def explore_list_of_list(row):
    try:
       d = row.asDict()
    except:
       d = row
    keys = d.keys()
    for row in [dict(zip(keys, vals)) for vals in zip(*(d[k] for k in keys))]:
       yield row
       

# Method 4:   
def explore_list_of_list(row):
    try:
       d = row.asDict()
    except:
       d = row
    keys = d.keys()
    for row in [dict(zip(keys, vals)) for vals in zip(*d.values())]:
       yield row

rdd_new = df_new.rdd.flatMap(lambda x: explode_lol(x))
df1 = rdd_new.toDF(df_new.columns)   # 114931/29



# Save truncated data
# hided


# =============================================================================
# Predictors and response variables
# =============================================================================
# Classify columns 
# del_col = sth   # hided

# For Response Variable: keep the last element
## "transaction_date_list" needs further discussion
def get_last(x):
    if x:
        return x[-1]
    else:
        return None
mylast = F.udf(lambda x: get_last(x), FloatType())
df = df.withColumn('Y_gtv', mylast('delta_gtv_list'))
df = df.select([column for column in df.columns if column not in del_col])   # 114931/25

# For Predictors: remove last element
pred_col = list(set().union(sales_col, visits_col) - set(del_col))
## Define function
def remove_end(x, colnames):
    d = x.asDict()      
    for col in colnames:
        d[col].pop() 
    return d

## Execute function
rdd_pred = df.rdd.map(lambda x: remove_end(x, pred_col))
df_pred = spark.createDataFrame(rdd_pred)


# "dow": IntegerEncode
dow_dict = {"Monday":1, "Tuesday":2, "Wednesday":3, "Thursday":4, "Friday":5, "Saturday":6, "Sunday":7}

def mapping(list):
	if list: 
		return [dow_dict[x] for x in list]
	else:
		return None
mymap = F.udf(lambda x: mapping(x), ArrayType(IntegerType()))
df_pred = df_pred.withColumn("top_event_event_visit_dow_list", mymap("top_event_event_visit_dow_list"))

# "category": OnehotEncode
## Get the list of unique major category
cat_name = [x['major_category'] for x in df.select("major_category").distinct().collect()]

## Create new feature for each major category
for name in cat_name:
	def compare(x):
		if x:
			return [int(x_ele == name) for x_ele in x]
	myOnehotEncode = F.udf(lambda x: compare(x), ArrayType(IntegerType()))
	df_pred = df_pred.withColumn('major_category_'+str(name), myOnehotEncode("top_event_major_category_list"))     # 114931/41



# =============================================================================
# Final processed data
# =============================================================================
# id_col = sth     # hided
# todo_col = sth   # hided

rem_col = list(set().union(todo_col, id_col))

df = df.select([column for column in df.columns if column not in rem_col])
df.count()
len(df.columns)   # 28