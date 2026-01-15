"""
Question 2: What is the percentage of computational power lost due to maintenance?
(a machine went offline and reconnected later)

La puissance de calcul est proportionnelle à la capacité CPU et à la période d'indisponibilité.
"""

from config import *

def main():
    print("=" * 60)
    print("Question 2: Pourcentage de puissance perdue (maintenance)")
    print("=" * 60)
    
    sc = create_spark_context("Q2_Maintenance_Loss")
    
    try:
        # Charger les données machine_events
        machine_events = sc.textFile(MACHINE_EVENTS_PATH)
        
        # Parser et extraire (machine_id, timestamp, event_type, cpu)
        parsed_events = machine_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5) \
            .map(lambda x: (
                x[MACHINE_EVENTS_SCHEMA["machine_id"]],
                safe_long(x[MACHINE_EVENTS_SCHEMA["timestamp"]]),
                safe_int(x[MACHINE_EVENTS_SCHEMA["event_type"]]),
                safe_float(x[MACHINE_EVENTS_SCHEMA["cpu"]])
            ))
        
        parsed_events.cache()
        
        # Grouper les événements par machine
        events_by_machine = parsed_events \
            .map(lambda x: (x[0], [(x[1], x[2], x[3])])) \
            .reduceByKey(lambda a, b: a + b)
        
        def calculate_downtime_and_power(machine_data):
            """
            Calcule le temps d'indisponibilité et la puissance perdue pour une machine.
            
            Event types:
            0 = ADD (machine online)
            1 = REMOVE (machine offline - maintenance)
            2 = UPDATE (mise à jour des attributs)
            """
            machine_id, events = machine_data
            
            # Trier les événements par timestamp
            sorted_events = sorted(events, key=lambda x: x[0])
            
            total_downtime = 0
            cpu_capacity = 0
            remove_timestamp = None
            
            # Trouver le timestamp max pour la fin de l'observation
            max_timestamp = max(e[0] for e in sorted_events) if sorted_events else 0
            
            for timestamp, event_type, cpu in sorted_events:
                if cpu > 0:
                    cpu_capacity = cpu  # Mémoriser la capacité CPU
                
                if event_type == 1:  # REMOVE - machine offline
                    remove_timestamp = timestamp
                elif event_type == 0 and remove_timestamp is not None:  # ADD après REMOVE
                    # Machine revenue online
                    downtime = timestamp - remove_timestamp
                    total_downtime += downtime
                    remove_timestamp = None
            
            # Si la machine est toujours offline à la fin
            if remove_timestamp is not None:
                # On compte jusqu'à la fin du dataset (approximation)
                total_downtime += DATASET_DURATION_MICROSECONDS - remove_timestamp
            
            # Puissance perdue = CPU * downtime
            power_lost = cpu_capacity * total_downtime
            
            return (machine_id, cpu_capacity, total_downtime, power_lost)
        
        # Calculer pour chaque machine
        machine_stats = events_by_machine.map(calculate_downtime_and_power)
        machine_stats.cache()
        
        # Statistiques globales
        stats = machine_stats.collect()
        
        total_power_lost = sum(s[3] for s in stats)
        total_machines_with_downtime = sum(1 for s in stats if s[2] > 0)
        
        # Calculer la puissance totale théorique
        # = somme des (CPU * durée totale) pour chaque machine
        total_cpu_capacity = sum(s[1] for s in stats)
        total_theoretical_power = total_cpu_capacity * DATASET_DURATION_MICROSECONDS
        
        if total_theoretical_power > 0:
            percentage_lost = (total_power_lost / total_theoretical_power) * 100
        else:
            percentage_lost = 0
        
        print(f"\nNombre total de machines: {len(stats)}")
        print(f"Machines ayant subi une maintenance: {total_machines_with_downtime}")
        print(f"\nCapacité CPU totale (normalisée): {total_cpu_capacity:.2f}")
        print(f"Puissance théorique totale (CPU * temps): {total_theoretical_power:.2e}")
        print(f"Puissance perdue (CPU * downtime): {total_power_lost:.2e}")
        
        print("\n" + "=" * 60)
        print(f"POURCENTAGE DE PUISSANCE PERDUE: {percentage_lost:.4f}%")
        print("=" * 60)
        
        # Top 10 des machines avec le plus de downtime
        print("\nTop 10 des machines avec le plus de temps d'indisponibilité:")
        print("-" * 60)
        sorted_by_downtime = sorted(stats, key=lambda x: x[2], reverse=True)[:10]
        
        for machine_id, cpu, downtime, power_lost in sorted_by_downtime:
            downtime_hours = downtime / (MICROSECONDS_TO_SECONDS * 3600)
            print(f"  Machine {machine_id}: {downtime_hours:.2f} heures (CPU: {cpu:.3f})")
        
        # Analyse
        print("\n" + "=" * 60)
        print("ANALYSE:")
        print("=" * 60)
        print(f"""
Le pourcentage de puissance perdue de {percentage_lost:.4f}% signifie que:

1. Sur 29 jours d'observation, environ {percentage_lost:.2f}% de la capacité 
   de calcul totale a été perdue due à des machines offline.

2. Cela inclut:
   - Maintenance planifiée
   - Pannes matérielles
   - Mises à jour système

3. Ce chiffre est {'relativement faible' if percentage_lost < 5 else 'significatif'} 
   pour un cluster de cette taille, ce qui indique une 
   {'bonne' if percentage_lost < 5 else 'gestion à améliorer de la'} fiabilité 
   de l'infrastructure.
""")
        
        wait_for_user()
        
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
