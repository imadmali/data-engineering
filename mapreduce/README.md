# MapReduce

MapReduce is a useful data aggregation design pattern. At its core it involves two steps, a mapping step and a reducing step, in order to produce an aggregation that summarizes the input data (e.g. mean, count, sum, etc).

1. **Map**: Create key/value pairs where keys label the data in some way and values quantify these labels.
2. **Reduce**: Combine the values associated with the same keys.

These steps are conceptualized in the `mapreduce.py` script. The data aggregation involved is to calculate the word count from an excerpt of the science fiction book Dune. The `mapper` function takes a sentence in the excerpt, does some text cleaning, and calculates the word count. Each sentence that is input into the `mapper` function produces a word count dictionary. The final output of `map` is an array of dictionaries. 

The `reducer` function takes two dictionaries and combines them into a single dictionary. The `reduce` function applies this across the array output produced by the `map` function. The final result is a single dictionary representing the word count for the entire excerpt.

This example is highly stylized. The functions were applied sequentially over the input data; the first sentence was processed, followed by the second, and so on. The MapReduce design pattern really shines when each step can be executed in parallel over the data. Imagine trying to calculate the word count for an entire book or millions of documents. It would be inefficient to do this sequentially, one sentence at a time. When applying MapReduce in a parallel framework the mapper can assign key/value pairs to sets of input data simultaneously. Similar keys can then be sent to reducers to aggregate the associated values. This can also happen in parallel.

Parallel execution across data is how MapReduce works in Hadoop. Although a few more steps are involved:

1. **Map**: In parallel create key/value pairs where keys label the data in some way and values quantify these labels.
2. **Combine**: Do some local aggregation where possible.
3. **Shuffle**: Move data around so that identical keys are located on the same process/node (this is usually an expensive operation). 
4. **Reduce**: Perform the final aggregation by combining values that share the same key.

The Hadoop MapReduce [tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html) goes over this in detail with an example.

> A MapReduce job usually splits the input data-set into independent chunks which are processed by the map tasks in a completely parallel manner. The framework sorts the outputs of the maps, which are then input to the reduce tasks.

> **Mapper** maps input key/value pairs to a set of intermediate key/value pairs.

> Users can optionally specify a **combiner**... to perform local aggregation of the intermediate outputs, which helps to cut down the amount of data transferred from the Mapper to the Reducer.

> [**Shuffle**] Input to the Reducer is the sorted output of the mappers. In this phase the framework fetches the relevant partition of the output of all the mappers, via HTTP.

> **Reducer** reduces a set of intermediate values which share a key to a smaller set of values.