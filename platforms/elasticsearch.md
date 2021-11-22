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
--data-binary @fct_put.json http://localhost:9200/fct_table/_bulk
```

List all indices. This can be done in a couple ways.

```bash
curl http://localhost:9200/_cat/indices?v=true
curl http://localhost:9200/_aliases?pretty=true
```

Get the schema of an index. This is similar to the `DESCRIBE` command in SQL.

```bash
curl curl http://localhost:9200/fct_table/_mapping?pretty=true
```

## Select

```bash
curl -H 'Content-Type: application/json' \
-d @search.json http://localhost:9200/fct_mapping/_search?pretty=true
```

```json
{
    "_source": ["id", "v1"],
    "size": 10
}
```

## Sorting

## Filter/Where

## Group By