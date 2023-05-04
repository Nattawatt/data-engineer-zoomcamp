before run debug make sure that

- `profiles.yml` in  `/.dbt/`
- `google_credentials.json` in `/.google/credentials/`
profiles.yml

```yml
# profiles.yml
express_way:
  target: prod
  outputs:
    prod:
      dataset: express_way
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      location: asia-southeast1
      method: service-account
      priority: interactive
      project: <project name>
      threads: 4
      type: bigquery
```

# Run init project
```bash
docker compose build
```
```bash
docker compose run --workdir="//usr/app/dbt" dbt-bq init
```
```bash
docker compose run --workdir="//usr/app/dbt/express_way" dbt-bq debug
```

# useful command

build
```bash
docker compose run --workdir="//usr/app/dbt/express_way" dbt-bq build
```

generate docs
```bash
docker compose run --workdir="//usr/app/dbt/express_way" dbt-bq docs generate
```

open docs
```bash
docker compose run -p 8081:8081 --workdir="//usr/app/dbt/express_way" dbt-bq docs serve --port 8081
```