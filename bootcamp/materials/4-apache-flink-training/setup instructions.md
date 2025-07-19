setup 

1. in ~/data-engineer-handbook/bootcamp/materials/4-apache-flink-training 
Copy `example.env` to `flink-env.env`.

    ```bash
    cp example.env flink-env.env
    ```

2. in flink-env.env to get:
- KAFKA_WEB_TRAFFIC_SECRET, KAFKA_WEB_TRAFFIC_KEY go to presentation slides, copy from pdf. 
- IP_CODING_KEY - register AT https://www.ip2location.io/ TO GET API KEY

3. run docker-compose from 4-apache-flink-training to build containers: jobmanager and taskmanager

```
docker-compose --env-file flink-env.env up --build --remove-orphans  -d
```

4. run docker-compose from week 1 (postgres + pgadmin)

5. on localhost:8081 can see flink gui

6. create table processed_events in pgAdmin (localhost:5050). script in 4-apache-flink-training/sql/init.sql

7. submit flink job (start_job.py):

```
docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/start_job.py --pyFiles /opt/src -d
```

or 
```
make job
```

8. in flink gui will see 1 running job. in pgAdmin in table processed_events will see data. 

