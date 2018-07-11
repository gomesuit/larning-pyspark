- sample data
  - http://jsonstudio.com/resources/
- 参考
  - https://blog.amedama.jp/entry/2017/05/05/021306
  - https://blog.amedama.jp/entry/2018/03/03/173257

```python
sqlContext =SQLContext(sc)
df = sqlContext.read.json("companies.json")
df.printSchema()
df.show(1)
df.take(1)
df.select()

rdd = df.rdd
```

```
spark-submit sample1.py
```
