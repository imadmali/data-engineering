# Elasticsearch

Elasticsearch is fairly different from a typical SQL database. The table below shows a mapping of some terms from Elasticsearch to SQL:

| Elasticsearch | SQL |
| --- | --- |
| Index | Table |
| Schema | Mapping |
| Document | Record |

The sections that follow show to to create an index in Elasticsearch and query it using their Query DSL (domains specific language).

## Create/Populate Index

Create an index. This is similar to `CREATE TABLE` in SQL. You're effectively specifying the schema of the index. The data types for each field are listed [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html).

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