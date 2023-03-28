import json
import datetime
from pyspark import SparkContext
def mapper(d):
    media = d.get('crawler_target')
    if media is not None:
        media = media.get('specific_resource_type')
    elif media is None and 'instagram' in d.get('link'):
        media = 'instagram'
    elif media is None:
        return []
    if media == 'facebook':
        res = []
        date = d.get('created_time').split('T')[0]
        res.append(((media,date),1))
        if len(d.get('comments')['data']) > 0:
            for i in d.get('comments')['data']:
                res.append(((media,i.get("created_time").split("T")[0]),1))
        return res
    elif media == 'instagram':
        date = datetime.datetime.utcfromtimestamp(int(d.get('created_time'))).strftime('%Y-%m-%d')
        return [((media,date),1)]
    elif media == 'twitter':
        date = datetime.datetime.strptime(d.get('created_at'), "%a %b %d %H:%M:%S %z %Y").strftime('%Y-%m-%d')
        return [((media,date),1)]
    elif media == 'youtube':
        date = d.get('snippet').get('publishedAt')
        if (date is not None):
            return [((media,date.split('T')[0]),1)]
    return []
def toCSVLine(data):
    return f"{data[0][0]},{data[0][1]},{data[1]}"

sc = SparkContext()


data = sc.wholeTextFiles("hdfs://localhost:9000/social_data")
data = data.flatMap(lambda x: json.loads(x[1]))


# data.checkpoint()
mapped = data.flatMap(mapper)
# mapped.checkpoint()
result = mapped.reduceByKey(lambda x,y: x+y)

csv = result.map(toCSVLine).sortBy(lambda x: x[0]).collect()
with open("result.csv", 'w') as f:
    for l in csv:
        print(l, file=f)