```markdown
## Exercise 10 — Questions

### 1. Which file format is most suitable for storing sensor samples?

Avro. The six sensors are always-on producers, each generating one new reading per second indefinitely — a continuous stream of small, individual writes, not a batch of data collected up front. Avro supports writing one record at a time as it's generated, which fits this pattern directly. Parquet, by contrast, needs the full dataset already assembled in memory before it writes anything in one shot — good for analytics on data you already have, not for a live stream with no known end. JSON could technically write one record at a time too, but lacks Avro's embedded schema and schema-evolution support, and produces much larger files at scale due to repeating field names in every record.

### 2. How will you design the folder structure of your sensor samples?

```
/data/raw/sensor_id=<1-6>/temporal_aspect=<real_time|edge_prediction>/year=<yyyy>/month=<mm>/day=<dd>/<correlation_id>.avro
```

Partitioning by `sensor_id` and `temporal_aspect` first lets later analysis jump straight to one sensor's data without scanning the others. Partitioning by date after that keeps each partition's file count bounded — six sensors at 1Hz means over 500,000 files a day across all sensors combined if left unpartitioned by time, which would overwhelm HDFS's NameNode (metadata for every single file has to fit in memory, per Lecture 2's GFS reading). One file per sample is acceptable for this exercise's scale; a production version would batch several seconds or minutes of readings into each file to cut that count down further.

### 3. Define a common schema for the fictive data sources.

Two layers: the sensor reading itself, and a message envelope wrapping it.

**Reading (`SensorObj`)**
- `sensor_id` — 1-6, identifies which station
- `modality` — the wattage value, −600 to 600 MW
- `unit` — "MW"
- `temporal_aspect` — "real_time" or, in future, "edge_prediction"

**Envelope (`PackageObj`, the actual Avro record)**
- `payload` — the reading above, JSON-encoded as a string
- `correlation_id` — a UUID unique to this message
- `created_at` — UTC timestamp of generation
- `schema_version` — an integer, starting at 1

This satisfies the exercise's two hard requirements: `correlation_id` gives every single message a unique identifier, and `temporal_aspect` being an open string field (rather than a hardcoded flag) plus `schema_version` existing as a safety net means a second temporal aspect can be introduced later without breaking anything already written.
```