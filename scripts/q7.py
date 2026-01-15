"""
question 7 : do tasks from the same job run on the same machine ?

we compare the number of tasks in a job to the number of unique machines used
"""
from config import *
import matplotlib.pyplot as plt
import numpy as np

def main():
    print("question 7")
    
    sc = create_spark_context("question 7")
    
    try:
        task_events = sc.textFile(TASK_EVENTS_PATH)
        
        # key = job_id
        # value = (set(machine_id), number of tasks)
        # we filter for event type 1 = schedule (to get only tasks that actually ran)
        
        def extract_locality_info(line):
            parts = line.split(',')
            try:
                if len(parts) > 5 and parts[TASK_EVENTS_SCHEMA["event_type"]] == '1':
                    mid = parts[TASK_EVENTS_SCHEMA["machine_id"]]
                    jid = parts[TASK_EVENTS_SCHEMA["job_id"]]
                    
                    # we skip tasks with no machine assigned
                    if mid:
                        return (jid, ({mid}, 1))
            except:
                pass
            return None

        mapped_events = task_events \
            .map(extract_locality_info) \
            .filter(lambda x: x is not None)

        # step 2 : we aggregate per job

        # reduce : union the sets and sum the counters
        def merge_job_stats(a, b):
            (machines_a, count_a) = a
            (machines_b, count_b) = b
            return (machines_a | machines_b, count_a + count_b)

        job_stats_rdd = mapped_events \
            .reduceByKey(merge_job_stats)
            
        # step 3 : we compute metrics

        # we convert to : (total_tasks, unique_machines)
        # we filter out jobs with only 1 task because they are "local" by definition
        multi_task_jobs = job_stats_rdd \
            .mapValues(lambda x: (len(x[0]), x[1])) \
            .filter(lambda x: x[1][1] > 1) \
            .values() \
            .collect()
            
        total_jobs_analyzed = len(multi_task_jobs)
        print(f"analyzed {total_jobs_analyzed} multitask jobs")

        # metric : spread=machines/tasks
        stats = []
        distributed_count = 0 # jobs spread across many machines
        colocated_count = 0   # jobs packed onto few machines
        
        for num_machines, num_tasks in multi_task_jobs:
            ratio = num_machines / num_tasks
            stats.append(ratio)
            
            # we take in consideration that if a job has 10 tasks and uses > 8 machines so it's distributed
            if ratio > 0.8:
                distributed_count += 1
            # and if it has 10 tasks and uses < 2 machines so it's colocated
            elif ratio < 0.2:
                colocated_count += 1
        
        # viz
        print(f"highly distributed (tasks spread out) : {distributed_count} ({distributed_count/total_jobs_analyzed*100:.1f}%)")
        print(f"\nhighly colocated (tasks grouped) : {colocated_count} ({colocated_count/total_jobs_analyzed*100:.1f}%)")
        print(f"\navg 'spread ratio' (machines/tasks) : {np.mean(stats):.2f}")
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        ax1.hist(stats, bins=50, color='#9b59b6', edgecolor='black', alpha=0.7)
        ax1.set_xlabel('spread ratio (machines/tasks)')
        ax1.set_ylabel('nbr of Jobs')
        ax1.set_title('distribution strategy')
        ax1.axvline(1.0, color='red', linestyle='--', label='1 task/machine')
        ax1.legend()
        
        mixed = total_jobs_analyzed - distributed_count - colocated_count
        labels = ['distributed', 'colocated', 'mixed']
        sizes = [distributed_count, colocated_count, mixed]
        colors = ['#3498db', '#e74c3c', '#95a5a6']
        
        ax2.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=90)
        ax2.set_title('locality classification')
        
        plt.tight_layout()
        plt.savefig("output/q7.png", dpi=150)
        
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()