import numpy as np
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
 
dt1 = {'one':[0.3, 1.2, 1.3, 1.5, 1.4, 1.0],'two':[0.6, 1.2, 1.7, 1.5, 1.4, 2.0]}
dt = sc.parallelize([ (k,) + tuple(v[0:]) for k,v in dt1.items()]).toDF()
dt.show()

# A tuple is a sequence of immutable Python objects. Tuples are sequences, just like lists. 
# The differences between tuples and lists are, the tuples cannot be changed unlike lists and tuples use parentheses, 
# whereas lists use square brackets. Creating a tuple is as simple as putting different comma-separated values.

# Tuples are immutable which means you cannot update or change the values of tuple elements. You are able to take portions of existing tuples to create new tuples

#--- Start of my Transpose Code --- 
# Grad data from first columns, since it will be transposed to new column headers
new_header = [i[0] for i in dt.select("_1").rdd.map(tuple).collect()]
 
# Remove first column from dataframe
dt2 = dt.select([c for c in dt.columns if c not in ['_1']])
 
# Convert DataFrame to RDD
rdd = dt2.rdd.map(tuple)
 
# Transpose Data
rddT1 = rdd.zipWithIndex().flatMap(lambda (x,i): [(i,j,e) for (j,e) in enumerate(x)])   # The enumerate() function adds a counter to an iterable. So for each element in cursor , a tuple is produced with (counter, element)
rddT2 = rddT1.map(lambda (i,j,e): (j, (i,e))).groupByKey().sortByKey()
rddT3 = rddT2.map(lambda (i, x): sorted(list(x), cmp=lambda (i1,e1),(i2,e2) : cmp(i1, i2)))
rddT4 = rddT3.map(lambda x: map(lambda (i, y): y , x))
 
# Convert back to DataFrame (along with header)
df = rddT4.toDF(new_header)
 
df.show()


# A function to transpose
def transpose_original(header, rdd):
	rddT1 = rdd.zipWithIndex().flatMap(lambda (x,i): [(i,j,e) for (j,e) in enumerate(x)])  
	rddT2 = rddT1.map(lambda (i,j,e): (j, (i,e))).groupByKey().sortByKey()
	rddT3 = rddT2.map(lambda (i, x): sorted(list(x), cmp=lambda (i1,e1),(i2,e2) : cmp(i1, i2)))
	rddT4 = rddT3.map(lambda x: map(lambda (i, y): y , x))
	df = rddT4.toDF(header)
	return df


