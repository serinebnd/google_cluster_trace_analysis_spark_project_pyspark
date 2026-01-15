"""
questions 4 and 5 are analysis of workload distribution and stability

Q4 : we count how many jobs/tasks belong to each priority level (scheduling class)
Q5 : determine how unstable the cluster is (kill/eviction rates)
"""

from config import *
import matplotlib.pyplot as plt

def main():
    
    sc = create_spark_context("question 4 and 5")
    
    try:
        # 1st part : question 4
        
        job_events = sc.textFile(JOB_EVENTS_PATH)
        
        # here we only care about the submit event (type 0)so we are sure that we count each job exactly one time
        jobs_by_class = job_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5) \
            .filter(lambda x: safe_int(x[JOB_EVENTS_SCHEMA["event_type"]]) == 0) \
            .map(lambda x: (safe_int(x[JOB_EVENTS_SCHEMA["scheduling_class"]]), 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey() \
            .collect()
            

        task_events = sc.textFile(TASK_EVENTS_PATH)
        
        # same thing we filter for submit (type 0) to count unique tasks
        tasks_by_class = task_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 8) \
            .filter(lambda x: safe_int(x[TASK_EVENTS_SCHEMA["event_type"]]) == 0) \
            .map(lambda x: (safe_int(x[TASK_EVENTS_SCHEMA["scheduling_class"]]), 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey() \
            .collect()

        # prints the distribution results
        total_jobs = sum(count for _, count in jobs_by_class)
        print(f"jobs (total =  {total_jobs}) :")
        for cls, count in jobs_by_class:
            print(f"\tclass {cls}: {count:<8} ({count/total_jobs*100:.1f}%)")
            
        total_tasks = sum(count for _, count in tasks_by_class)
        print(f"\ntasks (total = {total_tasks}) :")
        for cls, count in tasks_by_class:
            print(f"\tclass {cls}: {count:<8} ({count/total_tasks*100:.1f}%)")

        
        # 2nd part : question 5
        # note : to know if a job was killed/evicted we need to look at its history so we group all events by ID

        # since the number of jobs is reasonable so we can collect results and process the final stats in python
        job_lifecycles = job_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5) \
            .map(lambda x: (
                x[JOB_EVENTS_SCHEMA["job_id"]], 
                safe_int(x[JOB_EVENTS_SCHEMA["event_type"]])
            )) \
            .groupByKey() \
            .mapValues(list) \
            .collect()
        
        # now we analyze the job lifecycles
        j_killed = 0
        j_evicted = 0
        j_finished = 0
        
        for job_id, events in job_lifecycles:
            if 5 in events: j_killed += 1   # event 5 = kill
            if 2 in events: j_evicted += 1  # event 2 = evict
            if 4 in events: j_finished += 1 # event 4 = finish
            
        j_total = len(job_lifecycles)


        # here it's different from jobs because tasks are too numerous to collect to the driver so we must process the stats on the cluster using reduce()
        def analyze_task_history(events_iter):
            events = list(events_iter)
            # return tuple : (is_killed, is_evicted, is_finished, 1_count)
            k = 1 if 5 in events else 0
            e = 1 if 2 in events else 0
            f = 1 if 4 in events else 0
            return (k, e, f, 1)

        task_stats = task_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 8) \
            .map(lambda x: (
                (x[TASK_EVENTS_SCHEMA["job_id"]], x[TASK_EVENTS_SCHEMA["task_index"]]), 
                safe_int(x[TASK_EVENTS_SCHEMA["event_type"]])
            )) \
            .groupByKey() \
            .mapValues(analyze_task_history) \
            .values() \
            .reduce(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))
            
        t_killed, t_evicted, t_finished, t_total = task_stats
        
        
        # prints
        print(f"jobs summary :")
        print(f"\tkilled : {j_killed:<8} ({j_killed/j_total*100:.1f}%)")
        print(f"\tevicted : {j_evicted:<8} ({j_evicted/j_total*100:.1f}%)")
        print(f"\tfinished : {j_finished:<8} ({j_finished/j_total*100:.1f}%)")

        print(f"\ntasks summary :")
        print(f"\tkilled : {t_killed:<8} ({t_killed/t_total*100:.1f}%)")
        print(f"\tevicted : {t_evicted:<8} ({t_evicted/t_total*100:.1f}%)")
        print(f"\tfinished : {t_finished:<8} ({t_finished/t_total*100:.1f}%)")


        combined_failure = (t_killed + t_evicted) / t_total * 100
        print(f"\n{combined_failure:.1f}% of tasks are interrupted")

        # step 3 : plots and viz
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # jobs per class
        axes[0, 0].bar([str(c) for c, _ in jobs_by_class], [n for _, n in jobs_by_class], color='#3498db')
        axes[0, 0].set_title('Q4 : job distribution by priority')
        axes[0, 0].set_xlabel('scheduling class')
        axes[0, 0].set_ylabel('count')

        # tasks per class
        axes[0, 1].bar([str(c) for c, _ in tasks_by_class], [n for _, n in tasks_by_class], color='#9b59b6')
        axes[0, 1].set_title('Q4 : task distribution by priority')
        axes[0, 1].set_xlabel('scheduling class')
        
        # job outcomes
        j_data = [j_killed, j_evicted, j_finished]
        j_labels = ['killed', 'evicted', 'finished']
        # Filter out zeros for cleaner chart
        valid_j = [(v, l) for v, l in zip(j_data, j_labels) if v > 0]
        axes[1, 0].pie([v for v, l in valid_j], labels=[l for v, l in valid_j], autopct='%1.1f%%', colors=['#e74c3c', '#f39c12', '#2ecc71'])
        axes[1, 0].set_title('Q5 : Job final status')

        # task outcomes
        t_data = [t_killed, t_evicted, t_finished]
        t_labels = ['killed', 'evicted', 'finished']
        valid_t = [(v, l) for v, l in zip(t_data, t_labels) if v > 0]
        axes[1, 1].pie([v for v, l in valid_t], labels=[l for v, l in valid_t], autopct='%1.1f%%', colors=['#e74c3c', '#f39c12', '#2ecc71'])
        axes[1, 1].set_title('Q5 : task final status')

        plt.tight_layout()
        out_path = "./output/q4_q5.png"
        plt.savefig(out_path, dpi=150)
        
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()