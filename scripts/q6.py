"""
question 6 : do tasks with a low scheduling class have a higher probability of being evicted?

the code script analyzes if tasks with a lower scheduling class are evicted more frequently than those with a higher scheduling class
it calculates the eviction rate only for tasks that were actually scheduled
"""
from config import *
import matplotlib.pyplot as plt
import numpy as np

def main():
    print("Question 6")
    
    # initializing spark context
    sc = create_spark_context("question 6")
    
    try:
        # Load the raw CSV data for task events
        task_events = sc.textFile(TASK_EVENTS_PATH)
        
        # step 1 : the goal is to identify unique tasks and their lifecycle events
        parsed_tasks = task_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 8) \
            .map(lambda x: (
                # key : unique task ID (job_id + task_index)
                (x[TASK_EVENTS_SCHEMA["job_id"]], x[TASK_EVENTS_SCHEMA["task_index"]]),
                # value : (event_type, scheduling_class)
                safe_int(x[TASK_EVENTS_SCHEMA["event_type"]]),
                safe_int(x[TASK_EVENTS_SCHEMA["scheduling_class"]])
            ))
        
        parsed_tasks.cache() # cache this RDD because we are about to perform a heavy shuffle (groupByKey)
        
        # step 2 : we need to group all events belonging to the same task together
        # example : ((job_id, task_id), [(event_A, class_A), (event_B, class_B), etc...])
        task_history = parsed_tasks \
            .map(lambda x: (x[0], (x[1], x[2]))) \
            .groupByKey() \
            .mapValues(list)

        # step 3 : analyzing the lifecycle of each task
        def analyze_lifecycle(events):
            """
            we will look at the full history of a task to determine :
            1) its scheduling class (priority)
            2) if it was ever "scheduled" (event_type = 1)
            3) if it was ever "evicted" (event_type = 2)
            """
            scheduling_class = 0
            was_evicted = False
            was_scheduled = False
            
            for event_type, sched_class in events:
                if sched_class > 0:
                    scheduling_class = sched_class
                
                if event_type == 1:
                    was_scheduled = True
                if event_type == 2:
                    was_evicted = True
            
            return (scheduling_class, was_evicted, was_scheduled)
        
        # apply these analysis to every task
        task_outcomes = task_history \
            .mapValues(analyze_lifecycle) \
            .values() \
            .filter(lambda x: x[2])  # we take in consideration only the tasks that were scheduled but they can not be evicted if they never started running
        
        task_outcomes.cache()
        
        # step 4 : aggregate eviction statistics by scheduling class
        # map to : (sched_class, (total_scheduled_tasks, total_evicted_tasks))
        # reduce : we do the sum of the counters
        eviction_stats = task_outcomes \
            .map(lambda x: (x[0], (1, 1 if x[1] else 0))) \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .sortByKey() \
            .collect()
        
        # step 5 : display results and plots
        print("\nEviction probability by scheduling class :")
        print(f"\n{'sched_class':<15} {'total_tasks':<15} {'evicted_count':<15} {'eviction_rate':<15}")
        
        classes = []
        rates = []
        
        for sched_class, (total, evicted) in eviction_stats:
            rate = (evicted / total * 100) if total > 0 else 0
            classes.append(sched_class)
            rates.append(rate)
            print(f"{sched_class:<15} {total:<15} {evicted:<15} {rate:.2f}%")
        
        plt.figure(figsize=(10, 6))
        plt.bar([str(c) for c in classes], rates, color='indianred', alpha=0.7)
        plt.xlabel('Scheduling class (priority)')
        plt.ylabel('eviction rate (%)')
        plt.title('eviction probability by scheduling class')
        plt.grid(axis='y', linestyle='--', alpha=0.5)
        
        if len(classes) > 1:
            z = np.polyfit(classes, rates, 1)
            p = np.poly1d(z)
            plt.plot([str(c) for c in classes], [p(c) for c in classes], 
                     "b--", linewidth=2, label=f"Trend (slope: {z[0]:.2f})") # to visualize the correlation
            plt.legend()
        
        output_file = "./output/q6.png"
        plt.tight_layout()
        plt.savefig(output_file, dpi=150)
        
        # step 6 : analysis more deep (additional)

        print("\nAnalysis :")

        
        if rates:
            # we compare lowest priority vs highest priority
            low_prio_rate = rates[0] if 0 in classes else 0
            high_prio_rate = rates[-1] if len(rates) > 0 else 0
            
            trend = "stable"
            if len(classes) > 1:
                slope = np.polyfit(classes, rates, 1)[0]
                if slope < -0.5: trend = "decreasing"
                elif slope > 0.5: trend = "increasing"
        
            conclusion = ""
            if low_prio_rate > high_prio_rate:
                conclusion = "low scheduling classes have MUCH HIGHER eviction rate"
            elif low_prio_rate < high_prio_rate:
                conclusion = "unexpectedly higher classes are evicted more often"
            else:
                conclusion = "the eviction rate seems independent of the scheduling class"

            print(f"""
            findings :
            - low priority eviction rate (class 0): {low_prio_rate:.2f}%
            - high Priority eviction rate (class {classes[-1]}): {high_prio_rate:.2f}%
            - general trend: {trend}

            conclusion :
            {conclusion}
            """)
        
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()