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