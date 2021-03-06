# MapReduce

MapReduce is a useful data aggregation design pattern. It essentially involves two steps, a mapping step and a reducing step, in order to produce an aggregation that aggregates the input data in some way (e.g. mean, count, sum, etc).

1. **Map**: The map step involves creating key/value pairs, where the keys label the data in some way and the values quantify these labels.
2. **Reduce**: The reduce step combines the values associated with matching keys.

These steps are conceptualized in the `mapreduce.py` script.

```python
# MapReduce counting conceptualized with Python map/reduce functions
from functools import reduce
from collections import Counter
import json

data = ["Fear is the mind-killer",
		"Fear is the little-death that brings total obliteration",
		"I will face my fear",
		"I will permit it to pass over me and through me"]

# map function
def mapper(obj):
	clean_obj = obj.lower().split()
	return(Counter(clean_obj))
# reduce function
def reducer(obj, new_obj):
	obj.update(new_obj)
	return(obj)

# 1. A mapper (+ combiner) on each node maps a key (word) to a value (count)
mapped_data = list(map(mapper, data))
# 2. A shuffle normally takes place here to get similar keys
# on the same nodes.
# 3. A reducer combines the values associated with similar keys into final counts. 
reduced_data = reduce(reducer, mapped_data)

print(json.dumps(reduced_data, indent=4))
```

The data aggregation involved in this example is to calculate the word count from an excerpt of the science fiction book Dune. The `mapper` function takes a sentence in the excerpt, does some text cleaning, and calculates the word count. To be more transparent, two distinct steps are actually taking place: map _and_ combine (see below for details). In a more straightforward MapReduce framework the `mapper` function would only assign a value of 1 to each word (i.e. mapping without any combining). Each sentence that is input into the `mapper` function produces a word count dictionary. The final output of `map` is an array of dictionaries. 

The `reducer` function takes two dictionaries and combines them into a single dictionary. The `reduce` function applies the `reducer` function over the array output produced by the `map` function. The final result is a single dictionary representing the word count for the entire excerpt.

This example is highly stylized. The functions were applied sequentially over the input data; the first sentence was processed, followed by the second, and so on. The MapReduce design pattern really shines when each step can be executed in parallel over the data. Imagine trying to calculate the word count for an entire book or millions of documents. It would be inefficient to do this sequentially, one sentence at a time. When running MapReduce in a parallel framework the mapper can assign key/value pairs to sets of input data simultaneously. Similar keys can then be sent to reducers to aggregate the associated values. This can also happen in parallel.

MapReduce in Hadoop utilizes parallel execution over the data. Although a few more steps are involved:

1. **Map**: In parallel, create key/value pairs where keys label the data in some way and values quantify these labels.
2. **Combine**: Do some local aggregation where possible.
3. **Shuffle**: Move data around so that identical keys are located on the same process/node (this is usually an expensive operation). 
4. **Reduce**: Perform the final aggregation by combining values that share the same key.

The Hadoop MapReduce [tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html) goes over this in detail with an example.

> A MapReduce job usually splits the input data-set into independent chunks which are processed by the map tasks in a completely parallel manner. The framework sorts the outputs of the maps, which are then input to the reduce tasks.

> **Mapper** maps input key/value pairs to a set of intermediate key/value pairs.

> Users can optionally specify a **combiner**... to perform local aggregation of the intermediate outputs, which helps to cut down the amount of data transferred from the Mapper to the Reducer.

> [**Shuffle**] Input to the Reducer is the sorted output of the mappers. In this phase the framework fetches the relevant partition of the output of all the mappers, via HTTP.

> **Reducer** reduces a set of intermediate values which share a key to a smaller set of values.