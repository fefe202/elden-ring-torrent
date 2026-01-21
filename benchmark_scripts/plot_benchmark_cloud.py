#!/usr/bin/env python3
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import seaborn as sns

class BenchmarkPlotter:
    def __init__(self, output_dir="plots"):
        self.data = {}
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Apply style
        sns.set_theme(style="whitegrid")
        plt.rcParams.update({'figure.figsize': (10, 6)})

    def load_data(self, filepath_map):
        """
        Load JSON data from a map of Label -> Filepath.
        Example: {"Naive": "path/to/naive.json", "Metadata": ...}
        """
        for label, path in filepath_map.items():
            if not os.path.exists(path):
                print(f"Warning: File not found: {path} (Skipping {label})")
                continue
            
            try:
                with open(path, "r") as f:
                    self.data[label] = json.load(f)
                print(f"Loaded {label} from {path}")
            except Exception as e:
                print(f"Error loading {path}: {e}")

    def _get_percentile(self, data, p):
        return np.percentile(data, p * 100) if data else 0

    def plot_latency_comparison(self):
        """Bar chart comparing Avg and P99 Search Latencies."""
        labels = []
        avg_vals = []
        p99_vals = []
        
        for label, d in self.data.items():
            latencies = d.get("search_latencies", [])
            if not latencies: continue
            
            labels.append(label)
            avg_vals.append(np.mean(latencies))
            p99_vals.append(self._get_percentile(latencies, 0.99))
            
        x = np.arange(len(labels))
        width = 0.35
        
        fig, ax = plt.subplots()
        rects1 = ax.bar(x - width/2, avg_vals, width, label='Avg Latency', color='steelblue')
        rects2 = ax.bar(x + width/2, p99_vals, width, label='P99 Latency', color='salmon')
        
        ax.set_ylabel('Time (s)')
        ax.set_title('Search Latency Comparison (Avg vs P99)')
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()
        
        path = os.path.join(self.output_dir, "latency_comparison_bar.png")
        plt.savefig(path)
        plt.close()
        print(f"Saved {path}")

    def plot_latency_cdf(self):
        """Line chart showing CDF of search latencies."""
        plt.figure()
        
        for label, d in self.data.items():
            latencies = sorted(d.get("search_latencies", []))
            if not latencies: continue
            
            yvals = np.arange(len(latencies)) / float(len(latencies) - 1)
            plt.plot(latencies, yvals, label=f"{label} (n={len(latencies)})", linewidth=2)
            
        plt.xlabel('Latency (s)')
        plt.ylabel('CDF')
        plt.title('Search Latency CDF')
        plt.legend()
        plt.grid(True)
        
        path = os.path.join(self.output_dir, "latency_cdf.png")
        plt.savefig(path)
        plt.close()
        print(f"Saved {path}")

    def plot_throughput(self):
        """Bar chart of Request Throughput."""
        labels = []
        th_vals = []
        
        for label, d in self.data.items():
            latencies = d.get("search_latencies", [])
            if not latencies: continue
            
            total_time = sum(latencies)
            # Try to get pre-calculated throughput from metrics (local benchmark support)
            metrics = d.get("metrics", {})
            if "throughput" in metrics:
                th = metrics["throughput"]
            else:
                # Fallback: Client-side throughput approximation
                th = len(latencies) / total_time if total_time > 0 else 0
            
            labels.append(label)
            th_vals.append(th)
            
        plt.figure()
        sns.barplot(x=labels, y=th_vals, palette="viridis")
        plt.ylabel('Throughput (req/s)')
        plt.title('Client-Side Throughput Comparison')
        
        path = os.path.join(self.output_dir, "throughput_comparison.png")
        plt.savefig(path)
        plt.close()
        print(f"Saved {path}")

    def plot_load_balance_gini(self):
        """Bar chart for Gini Coefficient (Storage Load Balance)."""
        labels = []
        gini_vals = []
        
        for label, d in self.data.items():
            metrics = d.get("metrics", {})
            gini = metrics.get("gini", 0)
            
            labels.append(label)
            gini_vals.append(gini)
            
        plt.figure()
        sns.barplot(x=labels, y=gini_vals, palette="magma")
        plt.ylabel('Gini Coefficient (Lower is Better)')
        plt.title('Storage Load Imbalance (Gini Coefficient)')
        
        path = os.path.join(self.output_dir, "load_balance_gini.png")
        plt.savefig(path)
        plt.close()
        print(f"📊 Saved {path}")

    def plot_upload_latency(self):
        """Box plot for Upload Latencies."""
        all_data = []
        all_labels = []
        
        for label, d in self.data.items():
            u_lat = d.get("upload_latencies", [])
            if not u_lat: continue
            
            all_data.extend(u_lat)
            all_labels.extend([label] * len(u_lat))
            
        if not all_data: return

        plt.figure()
        sns.boxplot(x=all_labels, y=all_data, palette="Set2", showfliers=False)
        plt.ylabel('Time (s)')
        plt.title('Upload Latency Distribution (Outliers Hidden)')
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        
        path = os.path.join(self.output_dir, "upload_latency_boxplot.png")
        plt.savefig(path)
        plt.close()
        print(f"Saved {path}")

    def run_all(self):
        if not self.data:
            print("No data loaded. Exiting.")
            return

        print("Generating Plots...")
        self.plot_latency_comparison()
        self.plot_latency_cdf()
        self.plot_throughput()
        self.plot_load_balance_gini()
        self.plot_upload_latency()
        print("All plots generated.")

if __name__ == "__main__":
    # Define mapping
    files = {
        "Naive": "benchmark_results_NAIVE.json",
        "Metadata": "benchmark_results_METADATA.json",
        "Semantic": "benchmark_results_SEMANTIC.json"
    }
    
    plotter = BenchmarkPlotter(output_dir="plots_comparison")
    plotter.load_data(files)
    plotter.run_all()
