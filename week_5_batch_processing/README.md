# Option 1 : Using Windows
# Requirement
- windows 10
- hadoop-3.2.0
- jdk-11.0.17
- spark-3.0.3-bin-hadoop3.2

# How to install Pyspark

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing/setup

# How to start Pyspark

To run PySpark, we first need to add it to `PYTHONPATH`:

```bash
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
```

add py4j path, cversions of py4j
```bash
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH"
```

Then start jupyter notebook

```
pip install jupyter noteook
```

```bash
jupyter notebook
```

# Option 2 : Using docker

create `docker-compose.yml` file

```yml
services:
  pyspark-zoomcamp:
    image: jupyter/pyspark-notebook
    ports:
      # jupyter notebook
      - 8888:8888
      # spark
      - 4040:4040
    volumes:
      - ./src:/home/jovyan/src
```

```bash
docker-compose up
```

# Ref
docker jupyter-notebook with pyspark : https://github.com/jupyter/docker-stacks