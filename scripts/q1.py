"""
Question 1: What is the distribution of the machines according to their CPU capacity?
Can you explain (motivate) it?

Ce script analyse la distribution des machines selon leur capacité CPU normalisée (0-1).
"""

from config import *
import matplotlib.pyplot as plt
import os

os.makedirs("./output", exist_ok=True)
def main():
    print("=" * 60)
    print("Question 1: Distribution des machines par capacité CPU")
    print("=" * 60)
    
    # Créer le SparkContext
    sc = create_spark_context("Q1_CPU_Distribution")
    
    try:
        # Charger les données machine_events
        machine_events = sc.textFile(MACHINE_EVENTS_PATH)
        
        # Parser les lignes et extraire (machine_id, cpu, event_type)
        # On ne garde que les événements ADD (type 0) pour avoir les machines uniques
        machines = machine_events \
            .map(parse_csv_line) \
            .filter(lambda x: len(x) > 5 and x[MACHINE_EVENTS_SCHEMA["event_type"]] == "0") \
            .map(lambda x: (
                x[MACHINE_EVENTS_SCHEMA["machine_id"]],
                safe_float(x[MACHINE_EVENTS_SCHEMA["cpu"]])
            )) \
            .distinct()  # Une entrée par machine
        
        # Cacher car on va réutiliser
        machines.cache()
        
        # Compter le nombre total de machines
        total_machines = machines.count()
        print(f"\nNombre total de machines: {total_machines}")
        
        # Distribution des CPU
        # Grouper par valeur de CPU et compter
        cpu_distribution = machines \
            .map(lambda x: (x[1], 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey() \
            .collect()
        
        print("\nDistribution des capacités CPU:")
        print("-" * 40)
        print(f"{'CPU Capacity':<15} {'Count':<10} {'Percentage':<10}")
        print("-" * 40)
        
        for cpu, count in cpu_distribution:
            percentage = (count / total_machines) * 100
            print(f"{cpu:<15.4f} {count:<10} {percentage:<10.2f}%")
        
        # Statistiques
        cpu_values = machines.map(lambda x: x[1]).collect()
        avg_cpu = sum(cpu_values) / len(cpu_values) if cpu_values else 0
        min_cpu = min(cpu_values) if cpu_values else 0
        max_cpu = max(cpu_values) if cpu_values else 0
        
        print("\nStatistiques:")
        print(f"  - CPU moyen: {avg_cpu:.4f}")
        print(f"  - CPU min: {min_cpu:.4f}")
        print(f"  - CPU max: {max_cpu:.4f}")
        
        # Créer un histogramme
        plt.figure(figsize=(10, 6))
        cpus = [x[0] for x in cpu_distribution]
        counts = [x[1] for x in cpu_distribution]
        
        plt.bar(range(len(cpus)), counts, tick_label=[f"{c:.3f}" for c in cpus])
        plt.xlabel("Capacité CPU (normalisée)")
        plt.ylabel("Nombre de machines")
        plt.title("Distribution des machines par capacité CPU")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("./output/q1_cpu_distribution.png", dpi=150)
        print("\n✓ Graphique sauvegardé: output/q1_cpu_distribution.png")
        
        # Analyse / Motivation
        print("\n" + "=" * 60)
        print("ANALYSE ET MOTIVATION:")
        print("=" * 60)
        print("""
La distribution des machines par capacité CPU dans un cluster Google 
révèle généralement quelques observations clés:

1. HÉTÉROGÉNÉITÉ: Les clusters de production sont rarement homogènes.
   Les machines sont ajoutées au fil du temps avec différentes générations
   de matériel.

2. NORMALISATION: Les valeurs sont normalisées entre 0 et 1, où 1 représente
   la machine avec le plus de CPU. Cela permet de comparer facilement.

3. CLUSTERS DE VALEURS: On observe souvent des "clusters" autour de certaines
   valeurs, correspondant aux différentes générations ou types de machines
   achetées en lot.

4. STRATÉGIE D'ACHAT: La distribution reflète la stratégie d'achat de Google -
   généralement des achats en gros de machines identiques, d'où les pics
   à certaines valeurs.
""")
        
        wait_for_user()
        
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
