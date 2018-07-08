- sample data
  - http://jsonstudio.com/resources/

```python
sqlContext =SQLContext(sc)
df = sqlContext.read.json("companies.json")
```
