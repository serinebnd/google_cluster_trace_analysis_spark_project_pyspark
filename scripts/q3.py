"""
question 3 : is there a class of machines (by CPU) with a higher maintenance rate ?

our approach :
- group events by machine to reconstruct its history
- count how much times each machine was offline (event 1 = remove)
- group machines by their CPU capacity
- calculate the average number of failures/maintenances per machine for each CPU class
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import *
import matplotlib.pyplot as plt

def main():
    print("question 3")

    sc = create_spark_context("question 3")
    
    try:

        machine_events = sc.textFile(MACHINE_EVENTS_PATH)
        
        
        parsed_events = machine_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5) \
            .map(lambda x: (
                x[MACHINE_EVENTS_SCHEMA["machine_id"]], # key : machine_id
                (
                    safe_long(x[MACHINE_EVENTS_SCHEMA["timestamp"]]),
                    safe_int(x[MACHINE_EVENTS_SCHEMA["event_type"]]),
                    safe_float(x[MACHINE_EVENTS_SCHEMA["cpu"]])
                )
            ))

        # step 2 : we analyze each machine's history

        # groupby machine_id -> (id, [event1, event2,etc...])
        grouped_machines = parsed_events.groupByKey()
        
        def analyze_machine_lifecycle(data):
            """
            this function will : process the timeline of events for a single machine
            so we determine its CPU class and how many times it was taken offline.
            """
            machine_id, events_iter = data
            # we sort events by time to read the story in order
            events = sorted(list(events_iter), key=lambda x: x[0])
            
            cpu_capacity = 0
            maintenance_count = 0
            
            for ts, event_type, cpu in events:
                # we do update CPU capacity (usually established at "add" event)
                if cpu > 0:
                    cpu_capacity = cpu
                
                # event 1 = remove (machine = offline)
                # we interpret this as a maintenance event or failure
                if event_type == 1:
                    maintenance_count += 1
            
            # we bucketize CPU to create classes (example : 0.51 -> 0.5)
            cpu_class = round(cpu_capacity, 2)
            
            # return : (CPU_class, (1_machine, count_failures))
            return (cpu_class, (1, maintenance_count))

        # analysis
        machine_stats = grouped_machines \
            .map(analyze_machine_lifecycle) \
            .filter(lambda x: x[0] > 0) # we filter out machines with unknown CPU (0 capacity)
            
        # step 3 : we do aggregation by CPU_class
        
        # reduce : sum the machine counts and the maintenance counts
        class_stats = machine_stats \
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .sortByKey() \
            .collect()
            
        # output
        print(f"{'CPU class':<12} {'Number of machines':<15} {'total maintenance':<15} {'avrg rate':<15}")
        print("-" * 65)
        
        classes = []
        rates = []
        counts = []
        
        for cpu_cls, (nb_machines, nb_maint) in class_stats:
            # we only care about classes with "enough" machines to be statistically significant
            if nb_machines > 10: 
                avg_rate = nb_maint / nb_machines
                
                classes.append(cpu_cls)
                rates.append(avg_rate)
                counts.append(nb_machines)
                
                print(f"{cpu_cls:<12.2f} {nb_machines:<15} {nb_maint:<15} {avg_rate:<15.4f}")
        
        # we determine the worst reliability class
        if rates:
            max_rate = max(rates)
            worst_class = classes[rates.index(max_rate)]
            print(f"\nthe least reliable machines are class {worst_class} with {max_rate:.2f} maintenances/machine")

        fig, ax1 = plt.subplots(figsize=(12, 6))

        bars = ax1.bar([str(c) for c in classes], rates, color='#e67e22', alpha=0.8, label='maint rate')
        ax1.set_xlabel('CPU class (normalized cap)')
        ax1.set_ylabel('Avg maintenance events per machine', color='#d35400')
        ax1.tick_params(axis='y', labelcolor='#d35400')

        ax2 = ax1.twinx()
        ax2.plot([str(c) for c in classes], counts, color='#2980b9', marker='o', linestyle='--', label='machine count')
        ax2.set_ylabel('num of machines in cluster', color='#2980b9')
        ax2.tick_params(axis='y', labelcolor='#2980b9')
        
        plt.title('question 3')
        fig.legend(loc="upper right", bbox_to_anchor=(1,1), bbox_transform=ax1.transAxes)
        
        plt.tight_layout()
        plt.savefig("./output/q3_maintenance_by_cpu.png", dpi=150)
        
        wait_for_user()
        
    finally:
        sc.stop()

if __name__ == "__main__":
    main()