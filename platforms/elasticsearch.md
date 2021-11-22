# Elasticsearch

<img src="../images/elasticsearch-horizontal.png">

Elasticsearch is a data manipulation tool, but it doesn't really belong in the data-manipulation section. It's very different from a typical SQL database. It initially started as a way to efficiently search text data, but has evolved to support most data types. While true, I still feel that you're better off using more SQL-native tools as your main database (Postgres, Snowflake, etc).

One of the biggest shortcomings is how much it deviates from SQL. It has it's own query language that is specified in JSON. It also returns JSON objects. Depending on your data and query complexity you could end up with some heavily nested queries/results which can require extra effort to debug/parse.

The platform also isn't optimized for SQL-like joins. This requires you to throw out commonly used data models that are extremely useful to organize data and data relationships.

It's worth noting that Elasticsearch does allow you to execute SQL queries. But it is limited. For example, (at the time of writing) if you have a nested JSON you can't perform aggregations using SQL. You have to use their query language.

Additionally, all your requests to Elasticsearch are made through a REST API. While a lot of platforms do this, the user isn't usually exposed to it (it happens under the hood of the platform's UI or command line tools). If you're using Mac/Linux you can use the [curl](https://man7.org/linux/man-pages/man1/curl.1.html) utility to submit these requests.

With all that in mind you may find yourself using Elasticsearch, so it's useful to be familiar with the basics of the platform. I'll draw some parallels between Elasticsearch and SQL since SQL is commonly used for data manipulation. The table below shows a mapping of some high-level concepts from Elasticsearch to SQL.

<center>

| Elasticsearch | SQL |
| --- | --- |
| Index | Table |
| Schema | Mapping |
| Document | Record |

</center>

The next section shows how to create an index in Elasticsearch and populate it with data. The sections that follow show how to query that index using the Elasticsearch Query DSL (domain specific language).

## Create Index

The first step is to create an index. This is similar to `CREATE TABLE` in SQL. You're effectively specifying the schema of the index (you're not populating it with data just yet). The data types for each field are listed [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html).

```bash
curl -X PUT \
-H 'Content-Type: application/json' \
-d @fct_mapping.json http://localhost:9200/fct_table
```

No that the table is defined we can insert some records. This is similar to the `INSERT INTO` SQL command.

```bash
curl -X PUT \
-H 'Content-Type: application/json' \
--data-binary @fct_put.json http://localhost:9200/fct_table/_bulk?pretty=true
```

List all indices. This can be done in a couple ways.

```bash
curl http://localhost:9200/_cat/indices?v=true
curl http://localhost:9200/_aliases?pretty=true
```

Get the schema of an index. This is similar to the `DESCRIBE` command in SQL.

```bash
curl http://localhost:9200/fct_table/_mapping?pretty=true
```

Delete an index. This is similar to the `DROP TABLE` command in SQL.

```bash
curl -X DELETE http://localhost:9200/fct_table 
```

## Query DSL

The sections that follow will use query DSL. It's quite different from traditional SQL. You have to specify your queries in JSON format. Elasticsearch has extensive documentation on [query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) on their website.

## Select

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

## Sorting

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

## Filter/Where

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

## Group By

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
