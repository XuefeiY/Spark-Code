from pyspark.sql import Row
l = [('Ankit',25), ('Ankit',25), ('Ankit',25), ('Jalfaizy',22),('Jalfaizy',22), ('Jalfaizy',22), ('saurabh',20)]
rdd = sc.parallelize(l)
people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
schemaPeople = sqlContext.createDataFrame(people)
x = schemaPeople
x.show()

y = x.dropDuplicates()
y.show()

z = x.drop_duplicates()
z.show()