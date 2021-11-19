# Data Engineering Razors

🪒 Define your workflow in a DAG.

🪒 Minimize the responsibility of each DAG task.

🪒 Window functions will save you.

🪒 Use scheduled run timestamp.

🪒 Creating columns to understand transformations is like print debugging... remove them in prod.

🪒 Operate on ETL windows defined by the scheduled run timestamp.

🪒 Delete before you write.

🪒 Don't shuffle data unless it's required.

🪒 Build layers of aggregation: telemetry -> session -> ETL aggregate -> ...

🪒 Modularize your code into reusable components (UDFs are your friend here).

🪒 Unit test these components.

🪒 Test the pipeline end-to-end with data that mimics your source data.

🪒 Make sure there is logging for each DAG task and run/scheduled timestamps.

🪒 Keep your documentation close to your code.

🪒 Create alerts on failure.

🪒 Operate on a unit of time that is acceptable given the stakeholder's request _and_ the scale of the data.

🪒 Choose a smaller unit of time rather than larger (e.g. hourly over daily).

🪒 Clean up after yourself (e.g. with try/except clauses in Python).

🪒 Keep writes idempotent.

🪒 Let your pipeline do some (but not necessarily all) backfill.

🪒 Utilize partitioning built into your framework.

🪒 Don't wrap SQL transformations in strings.

🪒 Your prod role should be different than the role you develop with.

🪒 Discuss the schema with stakeholders before you start.

🪒 Use tools/frameworks that abstract your architecture rather than having one tool that does everything (where possible).

🪒 If storage costs < compute costs then it makes sense to pre-compute and store bigger tables, rather than doing this on the fly.