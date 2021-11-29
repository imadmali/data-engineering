<img src="../images/elasticsearch-horizontal.png">

# Elasticsearch

Elasticsearch is another data manipulation framework, but it doesn't really belong in the [data-manipulation](../data-manipulation/README.md) section. It differs from typical SQL databases (postgres) or SQL-like APIs (pyspark, pandas, dplyr). Elasticsearch considers sets of records rather than focusing on tables. It initially started as a way to efficiently search text data, but has evolved to support a variety of data types. In most situations you'll probably be better off using more SQL-native tools as your analytics data store (Postgres, Snowflake, etc). SQL is commonly known, and its table-based structure allows you to define well thought out data models to organize your data.

In my opinion, one of the biggest shortcomings of Elasticsearch is how much it deviates from SQL. It has it's own query language that is specified in JSON. Your queries also returns JSON objects, not tables. Depending on your data and query complexity you could end up with some heavily nested JSON objects that you have to manage. This leads to some extra debugging/parsing effort.

It's worth noting that there is a way to [wrap SQL queries](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-getting-started.html) in their REST API. But it is limited compared to SQL-native platforms. For example, (at the time of writing) if you have a nested JSON you can't perform aggregations on the nested fields using SQL. You have to use their custom query language.

Elasticsearch also isn't optimized for SQL-like joins. This requires you to throw out commonly used data models that are extremely useful at organizing data and data relationships.

Additionally, all your requests to Elasticsearch are made through a REST API. While a lot of platforms do this, the user isn't usually exposed to it (it happens under the hood of the platform's UI or command line tools). If you're using Mac/Linux you can use the [curl](https://man7.org/linux/man-pages/man1/curl.1.html) utility to submit these requests.

With all that in mind you may find yourself using Elasticsearch, so it's useful to be familiar with the basics of the platform. I'll draw some parallels between Elasticsearch and SQL since SQL is commonly used for data manipulation. The table below shows a mapping of some high-level concepts from Elasticsearch to SQL (see the Elasticsearch mapping [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/_mapping_concepts_across_sql_and_elasticsearch.html) for more detail).

<center>

| Elasticsearch | SQL |
| --- | --- |
| Index | Table |
| Schema | Mapping |
| Document | Record/Row |

</center>

The next section shows how to create an index in Elasticsearch and populate it with some data. The sections that follow show how to query that index using the Elasticsearch Query DSL (domain specific language).

I ran these commands against a simple docker container running Elasticsearch. Refer to the documentation [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html) to spin one up.

## Create and Populate Index

The first step is to create an index. This is similar to `CREATE TABLE` in SQL. You're effectively specifying the schema of the index (you're not populating it with data just yet). The data types for each field are listed [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html).

```bash
curl -X PUT \
-H 'Content-Type: application/json' \
-d @fct_mapping.json http://localhost:9200/fct_table
```

The `fct_mapping.json` request above creates an index with the following schema.

| Name | Type |
| --- | --- |
| id |  keyword (this gets normalized to lowercase) |
| v0 |  long |
| v1 |  long |
| v2 |  keyword (this gets normalized to lowercase) |

Now that the index is defined we can insert some documents. This is similar to the `INSERT INTO` SQL command, which is used to insert records into a table.

```bash
curl -X PUT \
-H 'Content-Type: application/json' \
--data-binary @fct_put.json http://localhost:9200/fct_table/_bulk?pretty=true
```

The format of the JSON request I'm sending looks like this.

```json
{"index" : {}}
{"id": "A", "v0": 4.162335551296317, "v1": 8, "v2": "N"}
{"index" : {}}
{"id": "E", "v0": -3.463043847744304, "v1": 3, "v2": "Y"}
{"index" : {}}
{"id": "A", "v0": 4.116661961802513, "v1": 2, "v2": "N"}
{"index" : {}}
{"id": "B", "v0": 2.1683668815994643, "v1": 4, "v2": "N"}
{"index" : {}}
{"id": "C", "v0": 5.940306422779212, "v1": 5, "v2": "Y"}
{"index" : {}}
{"id": "B", "v0": -5.350971634624138, "v1": 3, "v2": "Y"}
{"index" : {}}
{"id": "B", "v0": 3.094433010031948, "v1": 5, "v2": "N"}
{"index" : {}}
{"id": "B", "v0": -3.568400083224966, "v1": 6, "v2": "N"}
{"index" : {}}
{"id": "D", "v0": -6.691328447186232, "v1": 3, "v2": "Y"}
{"index" : {}}
{"id": "A", "v0": -5.293135681469833, "v1": 3, "v2": "N"}
```

Now that we've setup the data we can run some useful utility functions.

**List all indices available on the cluster**. This can be done in a couple ways.

```bash
curl http://localhost:9200/_cat/indices?v=true
curl http://localhost:9200/_aliases?pretty=true
```

**Get the schema of an index**. This is similar to the `DESCRIBE` command in SQL.

```bash
curl http://localhost:9200/fct_table/_mapping?pretty=true
```

**Delete an index**. This is similar to the `DROP TABLE` command in SQL.

```bash
curl -X DELETE http://localhost:9200/fct_table 
```

## Query DSL

The sections that follow will use Elasticsearch query DSL. It's quite different from traditional SQL. You have to specify your queries in JSON format and send them through a REST API. Elasticsearch has extensive documentation on [query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) on their website.

### Select

Return data from specified fields only.

```bash
curl -H 'Content-Type: application/json' \
-d @select.json http://localhost:9200/fct_table/_search?pretty=true
```

```json
{
    "_source": ["id", "v1"],
    "size": 10
}
```

### Sorting

Rearrange the data in ascending/descending order based on a specific field.

```bash
curl -H 'Content-Type: application/json' \
-d @sort.json http://localhost:9200/fct_table/_search?pretty=true
```

```json
{
    "size": 10,
    "sort": [
        {"v1": {"order": "asc"}}
    ]
}
```

### Filter/Where

Return data with certain documents included/excluded based on conditions applied against certain fields.

```bash
curl -H 'Content-Type: application/json' \
-d @filter.json http://localhost:9200/fct_table/_search?pretty=true
```

```json
{
    "size": 10,
    "query": {
        "bool": {
            "must": [
                {"term": {"id": "a"}},
                {"term": {"v1": 8}}
            ]
        }
    }
}
```

### Group By

Perform basic calculations on groups of fields. The nesting can get particularly messy for aggregate queries. Below we aggregate at the id-level. The aggregate `"A0"` calculates a count within each id group. The (nested) aggregate `"A1"` calculates the sum of the v1 field within each id group.

```bash
curl -H 'Content-Type: application/json' \
-d @groupby.json http://localhost:9200/fct_table/_search?pretty=true
```

```json
{
    "size": 0,
    "aggs": {
        "A0": {
            "terms": { "field": "id" },
            "aggs": {
                "A1": {
                    "sum": { "field": "v1" }
                }
            }
        }
    }
}
```

## Advanced Queries

We'll need to insert some more complex data into the database to explore some of the advanced queries listed below.

```bash
curl -X PUT \
-H 'Content-Type: application/json' \
-d @fct_mapping_nest.json http://localhost:9200/fct_table_nest

curl -X PUT \
-H 'Content-Type: application/json' \
--data-binary @fct_nest_put.json http://localhost:9200/fct_table_nest/_bulk?pretty=true
```

Here's the data we're inserting.

```json
{"index" : {}}
{"id": "A", "v0": 4.162335551296317, "words": [{"v1": "annihilation", "v2": "N"}]}
{"index" : {}}
{"id": "E", "v0": -3.463043847744304, "words": [{"v1": "authority", "v2": "Y"}]}
{"index" : {}}
{"id": "A", "v0": 4.116661961802513, "words": [{"v1": "acceptance", "v2": "N"}]}
{"index" : {}}
{"id": "B", "v0": 2.1683668815994643, "words": [{"v1": "the bone clocks", "v2": "N"}]}
{"index" : {}}
{"id": "C", "v0": 5.940306422779212, "words": [{"v1": "house of leaves", "v2": "Y"}]}
{"index" : {}}
{"id": "B", "v0": -5.350971634624138, "words": [{"v1": "ecotopia", "v2": "Y"}]}
{"index" : {}}
{"id": "B", "v0": 3.094433010031948, "words": [{"v1": "the forever war", "v2": "N"}]}
{"index" : {}}
{"id": "B", "v0": -3.568400083224966, "words": [{"v1": "a scanner darkly", "v2": "N"}]}
{"index" : {}}
{"id": "D", "v0": -6.691328447186232, "words": [{"v1": "the stars my destination", "v2": "Y"}]}
{"index" : {}}
{"id": "A", "v0": -5.293135681469833, "words": [{"v1": "three californias", "v2": "N"}]}
```

### Regexp/Wildcard Filter

Being able to filter using regular expression wildcard terms.

```bash
curl -H 'Content-Type: application/json' \
-d @filter_wildcard.json http://localhost:9200/fct_table_nest/_search?pretty=true
```

```json
{
    "size": 10,
    "query": {
        "bool": {
            "must": [
                {"regexp": {"id": "A.*|B.*"}}
            ]
        }
    }
}
```

### Nested Filter

```bash
curl -H 'Content-Type: application/json' \
-d @filter_nest.json http://localhost:9200/fct_table_nest/_search?pretty=true
```

```json
{
    "size": 10,
    "query": {
        "nested": {
            "path": "words",
            "query": {
                "bool": {
                    "must": [
                        {"match": {"words.v2": "y"}}
                    ]
                }
            }
        }
    }
}
```

### Group By Filter

```bash
curl -H 'Content-Type: application/json' \
-d @groupby_filter.json http://localhost:9200/fct_table_nest/_search?pretty=true
```

```json
{
    "size": 0,
    "aggs": {
        "A0": {
            "terms": {
                "field": "id",
                "exclude": ["A", "B"]
            }
        }
    }
}
```

### Group By Nested

```bash
curl -H 'Content-Type: application/json' \
-d @groupby_nested.json http://localhost:9200/fct_table_nest/_search?pretty=true
```

```json
{
    "size": 0,
    "aggs": {
        "A0": {
            "terms": { "field": "id" },
            "aggs": {
                "A1" : {
                    "nested": {"path": "words"},
                    "aggs": {
                        "A2": {
                            "terms": { "field": "words.v2" }
                        }
                    }
                }
            }
        }
    }
}
```
