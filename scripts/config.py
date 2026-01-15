from pyspark import SparkContext, SparkConf
import os

BASE_PATH = "../data"

MACHINE_EVENTS_PATH = os.path.join(BASE_PATH, "machine_events", "*.csv.gz")
TASK_EVENTS_PATH = os.path.join(BASE_PATH, "task_events", "*.csv.gz")
JOB_EVENTS_PATH = os.path.join(BASE_PATH, "job_events", "*.csv.gz")
TASK_USAGE_PATH = os.path.join(BASE_PATH, "task_usage", "*.csv.gz")
SCHEMA_PATH = os.path.join(BASE_PATH, "schema.csv")

# schema definitions (to not repeat numbers in code and check the schema everytime)

MACHINE_EVENTS_SCHEMA = {
    "timestamp": 0,
    "machine_id": 1,
    "event_type": 2,
    "platform_id": 3,
    "cpu": 4,
    "memory": 5
}

TASK_EVENTS_SCHEMA = {
    "timestamp": 0,
    "missing_info": 1,
    "job_id": 2,
    "task_index": 3,
    "machine_id": 4,
    "event_type": 5,
    "user": 6,
    "scheduling_class": 7,
    "priority": 8,
    "cpu_request": 9,
    "memory_request": 10,
    "disk_space_request": 11,
    "different_machines_restriction": 12
}

JOB_EVENTS_SCHEMA = {
    "timestamp": 0,
    "missing_info": 1,
    "job_id": 2,
    "event_type": 3,
    "user": 4,
    "scheduling_class": 5,
    "job_name": 6,
    "logical_job_name": 7
}

TASK_USAGE_SCHEMA = {
    "start_time": 0,
    "end_time": 1,
    "job_id": 2,
    "task_index": 3,
    "machine_id": 4,
    "cpu_rate": 5,
    "canonical_memory_usage": 6,
    "assigned_memory_usage": 7,
    "unmapped_page_cache": 8,
    "total_page_cache": 9,
    "maximum_memory_usage": 10,
    "disk_io_time": 11,
    "local_disk_space_usage": 12,
    "maximum_cpu_rate": 13,
    "maximum_disk_io_time": 14,
    "cycles_per_instruction": 15,
    "memory_accesses_per_instruction": 16,
    "sample_portion": 17,
    "aggregation_type": 18,
    "sampled_cpu_usage": 19
}

# we create a SparkContext with reduced memory settings and to not repeat code for each question
def create_spark_context(app_name="spark_project", master="local[1]"):
   
    conf = SparkConf().setAppName(app_name).setMaster(master)
    
    # for memory usage
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.memory", "2g")
    
    # for shuffle memory
    conf.set("spark.shuffle.file.buffer", "32k")
    conf.set("spark.reducer.maxSizeInFlight", "24m")

    # disable caching by default (we will enable it when needed)
    conf.set("spark.memory.fraction", "0.4")
    conf.set("spark.memory.storageFraction", "0.2")
    
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    return sc

# functions to use in processing data and avoid repetition

def safe_float(value, default=0.0):
    try:
        if value == "" or value is None:
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    try:
        if value == "" or value is None:
            return default
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_long(value, default=0):
    try:
        if value == "" or value is None:
            return default
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_csv_line(line):
    return line.split(",")


def wait_for_user():
    input("\nPress enter to continue\n")


# conversion constants (global variables)
DATASET_DURATION_MICROSECONDS = 29 * 24 * 60 * 60 * 1000000
MICROSECONDS_TO_SECONDS = 1000000