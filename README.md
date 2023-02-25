# Data Principal Task

You have two options for this task;
1) Timebox to an hour and present results during the technical interview.
2) Read the task beforehand and pair program a solution in ~30 minutes.

The company uses a tool called "Mudderstack" (very similar to [Rudderstack](https://www.rudderstack.com/)) to collect and distribute click stream events across each application.

Mudderstack supports delivery to the following source destinations,
- Batched to S3
- HTTP endpoints or Kafka on a per event basis.

Currently Mudderstack is configured to deliver events on a batch cycle, hourly to an S3 destination (in this case we've abstracted away this to use the local file system) - a sample is given in the `data` folder.


## Task One
A new business requirement has come in to summarise the amount of times an event has been viewed (indicated by the `event_viewed` property type) for each source and deliver this to a reporting database in hourly batches.

Please implement the methods in in `task.py` (plus any additional methods needed) and the tests in `tests/test_task.py`.
Feel free to change method signatures if required.

A sample of Mudderstack payload is as follows;
```
{
  "id": "5ee31063-e107-404c-8e7c-e4f1e09ea449",
  "ip": "21.33.54.112",
  "source": "android",
  "user_id": "3cc6870b-9f59-4e29-ab3a-ce2052d992db",
  "properties": {
    "type": "event_shared",
    "recieved_at": "2023-01-01T03:06:30",
    "event_name": "Fred Again: O2 Academy Brixton"
  }
}
```

A sample of these data can be generated using, this will be useful for a test fixture! ;)
```
python -m generate --day 2023-01-01
```

SQLite DB fixtures can be created with
```
python -m models
```
Feel free to refer to these models / connections where appropriate.

Running tests:
```
python -m pytest
````

The solution will be deployed as [Prefect flow](https://www.prefect.io/), running on a Kubernetes cluster. Note, no Prefect code, alarms or logging has to be written for the purpose of this task.


## Task Two
Over the period of a few months, the volume of clickstream data has increased 100x and you find your solution is no longer able to process this volume of data anymore, how would you go about resolving the situation?  Please write a few summary points to discuss. p.s. no code is required!


## Task Three
A new business requirement comes in to have data delivered to a Machine Learning application at a much lower latency (seconds instead of hourly), how can you support this?  Please write a few summary points to discuss. p.s. no code is required!
