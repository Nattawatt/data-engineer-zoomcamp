# Running Dbt by Docker

```bash
cd src
```

```bash
docker-compose build 
```

## testing connection
before run debug make sure that

- `profiles.yml` in  `/.dbt/`
- `google_credentials.json` in `/.google/credentials/`

profiles.yml
```yml
taxi_ride_ny:
  target: dev
  outputs:
    dev:
      dataset: taxi_dbt_staging
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      location: asia-southeast1
      method: service-account
      priority: interactive
      project: de-zoomcamp-376514
      threads: 4
      type: bigquery
```
google_credentials.json
create IAM service account with
- roles/bigquery.dataEditor
- roles/bigquery.jobUser

```bash
docker compose run --workdir="//usr/app/dbt/taxi_ride_ny" dbt-bq debug
```