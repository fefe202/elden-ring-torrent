#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import numpy as np
import os

class BenchmarkPlotter:
    def __init__(self, json_file="benchmark_results.json"):
        self.json_file = json_file
        self.data = {}
        self.modes = []
        self.colors = {
            "NAIVE": "#3498db",      # Blu
            "METADATA": "#2ecc71",   # Verde
            "SEMANTIC": "#e74c3c"    # Rosso
        }
        
        if os.path.exists(json_file):
            with open(json_file, "r") as f:
                self.data = json.load(f)
            self.modes = list(self.data.keys())
        else:
            print(f" File {json_file} non trovato. Esegui prima benchmark_suite.py")

    def plot_latency_comparison(self):
        """
        Confronta i tempi medi di Upload e Search.
        """
        print("Generazione grafico: Latency Comparison...")
        
        modes = self.modes
        avg_uploads = []
        avg_searches = []
        std_uploads = []
        std_searches = []

        for m in modes:
            up_times = self.data[m]["upload_latency"]
            search_times = self.data[m]["search_latency"]
            
            avg_uploads.append(np.mean(up_times) if up_times else 0)
            std_uploads.append(np.std(up_times) if up_times else 0)
            
            avg_searches.append(np.mean(search_times) if search_times else 0)
            std_searches.append(np.std(search_times) if search_times else 0)

        x = np.arange(len(modes))
        width = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))
        rects1 = ax.bar(x - width/2, avg_uploads, width, yerr=std_uploads, label='Upload (Write)', alpha=0.8, capsize=5)
        rects2 = ax.bar(x + width/2, avg_searches, width, yerr=std_searches, label='Search (Read)', alpha=0.8, capsize=5)

        ax.set_ylabel('Tempo (secondi)')
        ax.set_title('Confronto Prestazionale: Scrittura vs Lettura')
        ax.set_xticks(x)
        ax.set_xticklabels(modes)
        ax.legend()
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        self._autolabel(rects1, ax)
        self._autolabel(rects2, ax)

        plt.tight_layout()
        plt.savefig("plot_latency_avg.png")
        plt.show()

    def plot_storage_imbalance(self):
        """
        Mostra quanti file (chunk + indici) ci sono su ogni nodo.
        Serve a evidenziare gli HOTSPOT (Semantic) vs BILANCIAMENTO (Naive/Metadata).
        """
        print("Generazione grafico: Storage Imbalance...")
        
        num_peers = len(self.data[self.modes[0]]["metrics"]["storage_loads"])
        peer_indices = np.arange(num_peers)
        
        fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=True)
        fig.suptitle('Distribuzione del Carico (File per Nodo) - Load Balancing', fontsize=16)

        for i, mode in enumerate(self.modes):
            ax = axes[i]
            loads = self.data[mode]["metrics"]["storage_loads"]
            variance = self.data[mode]["metrics"].get("load_variance", 0)
            
            bars = ax.bar(peer_indices, loads, color=self.colors.get(mode, "gray"), alpha=0.7)
            
            ax.set_title(f"{mode}\nVariance: {variance:.2f}")
            ax.set_xlabel('Node ID')
            if i == 0:
                ax.set_ylabel('Numero Totale File (Chunk + Indici)')
            
            ax.grid(axis='y', linestyle='--', alpha=0.5)

        plt.tight_layout()
        plt.savefig("plot_storage_balance.png")
        plt.show()

    def plot_search_cdf(self):
        """
        Cumulative Distribution Function per la Search.
        Mostra la 'Qualità del Servizio' (QoS).
        """
        print("Generazione grafico: Search CDF...")
        
        plt.figure(figsize=(10, 6))

        for mode in self.modes:
            latencies = np.sort(self.data[mode]["search_latency"])
            # Calcola la probabilità cumulativa
            p = 1. * np.arange(len(latencies)) / (len(latencies) - 1)
            plt.plot(latencies, p, label=mode, color=self.colors.get(mode, "black"), linewidth=2)

        plt.title('CDF Latenza di Ricerca (Probabilità di risposta entro X secondi)')
        plt.xlabel('Tempo di Risposta (s)')
        plt.ylabel('Probabilità Cumulativa')
        plt.legend()
        plt.grid(True, which="both", ls="--")
        
        plt.tight_layout()
        plt.savefig("plot_search_cdf.png")
        plt.show()


    def plot_throughput(self):
        """
        Confronta il throughput (Saturation) misurato in req/s.
        """
        print("Generazione grafico: Saturation (Throughput)...")
        
        modes = self.modes
        throughputs = []

        for m in modes:
            # Recupera throughput pre-calcolato o calcolalo al volo
            metrics = self.data[m].get("metrics", {})
            th = metrics.get("throughput", 0)
            
            if th == 0:
                # Fallback calculation if missing in JSON
                lats = self.data[m]["search_latency"]
                total_time = sum(lats) if lats else 1
                th = len(lats) / total_time if total_time > 0 else 0
            
            throughputs.append(th)

        x = np.arange(len(modes))
        
        fig, ax = plt.subplots(figsize=(8, 6))
        colors = [self.colors.get(m, "gray") for m in modes]
        
        rects = ax.bar(x, throughputs, width=0.5, color=colors, alpha=0.9)

        ax.set_ylabel('Throughput (Richieste/Secondo)')
        ax.set_title('Saturazione del Sistema (Throughput)')
        ax.set_xticks(x)
        ax.set_xticklabels(modes)
        ax.grid(axis='y', linestyle='--', alpha=0.7)

        self._autolabel(rects, ax)

        plt.tight_layout()
        plt.savefig("plot_saturation.png")
        plt.show()

    def _autolabel(self, rects, ax):
        """Helper per mettere i numerini sopra le barre"""
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{height:.2f}',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom')

if __name__ == "__main__":
    plotter = BenchmarkPlotter()
    
    plotter.plot_latency_comparison()
    
    plotter.plot_storage_imbalance()
    
    plotter.plot_search_cdf()

    plotter.plot_throughput()
    
    print("Grafici salvati come PNG.")