import subprocess
import shutil
from time import sleep
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import json
from pathlib import Path
from collections import defaultdict
import requests
import string

class PeerMonitorGUI:
    # Temi colori moderni
    THEMES = {
        'dark': {
            'bg': '#121212', 'bg2': '#1e1e1e', 'bg3': '#252526',
            'accent': '#3b82f6', 'accent_hover': '#60a5fa',
            'text': '#e4e4e7', 'text2': '#a1a1aa',
            'success': '#10b981', 'success_hover': '#34d399',
            'warning': '#f59e0b', 'warning_hover': '#fbbf24',
            'error': '#ef4444', 'error_hover': '#f87171',
            'info': '#0ea5e9', 'info_hover': '#38bdf8',
            'purple': '#8b5cf6', 'purple_hover': '#a78bfa',
            'cyan': '#06b6d4', 'cyan_hover': '#22d3ee',
            'teal': '#14b8a6', 'teal_hover': '#2dd4bf',
            'indigo': '#6366f1', 'indigo_hover': '#818cf8',
            'pink': '#ec4899', 'pink_hover': '#f472b6',
            'secondary': '#64748b', 'secondary_hover': '#94a3b8'
        },
        'light': {
            'bg': '#ffffff', 'bg2': '#f3f4f6', 'bg3': '#e5e7eb',
            'accent': '#2563eb', 'accent_hover': '#3b82f6',
            'text': '#1f2937', 'text2': '#4b5563',
            'success': '#059669', 'success_hover': '#10b981',
            'warning': '#d97706', 'warning_hover': '#f59e0b',
            'error': '#dc2626', 'error_hover': '#ef4444',
            'info': '#0284c7', 'info_hover': '#0ea5e9',
            'purple': '#7c3aed', 'purple_hover': '#8b5cf6',
            'cyan': '#0891b2', 'cyan_hover': '#06b6d4',
            'teal': '#0d9488', 'teal_hover': '#14b8a6',
            'indigo': '#4f46e5', 'indigo_hover': '#6366f1',
            'pink': '#db2777', 'pink_hover': '#ec4899',
            'secondary': '#475569', 'secondary_hover': '#64748b'
        }
    }
    
    def __init__(self, root):
        self.root = root
        self.root.title("🌐 EldenRing Torrent - P2P Network")
        self.root.geometry("1400x850")
        self.root.state('zoomed')
        self.base_dir = Path(__file__).parent.parent
        self.dark_mode = True
        self.colors = self.THEMES['dark']
        
        self.root.configure(bg=self.colors['bg'])
        self.setup_styles()
        self.create_widgets()
        self.refresh_data()
        
    def setup_styles(self):
        """Configura stili moderni e minimalisti con hover effects"""
        style = ttk.Style()
        style.theme_use('clam')
        c = self.colors
        
        # Frame e Label base
        for name in ['Main', 'Card', 'Header']:
            style.configure(f'{name}.TFrame', background=c['bg2'] if name != 'Main' else c['bg'])
        
        style.configure('Title.TLabel', font=('Segoe UI', 24, 'bold'), 
                       foreground=c['text'], background=c['bg2'])
        style.configure('Subtitle.TLabel', font=('Segoe UI', 11), 
                       foreground=c['text2'], background=c['bg2'])
        style.configure('Status.TLabel', font=('Segoe UI', 10, 'bold'), 
                       foreground=c['success'], background=c['bg2'])
        
        # Mappa stili bottoni per hover
        btn_styles = [
            ('Modern', 'accent', 'accent_hover'),
            ('Success', 'success', 'success_hover'), 
            ('Warning', 'warning', 'warning_hover'),
            ('Danger', 'error', 'error_hover'),
            ('Info', 'info', 'info_hover'),
            ('Purple', 'purple', 'purple_hover'),
            ('Cyan', 'cyan', 'cyan_hover'),
            ('Teal', 'teal', 'teal_hover'),
            ('Indigo', 'indigo', 'indigo_hover'),
            ('Pink', 'pink', 'pink_hover'),
            ('Secondary', 'secondary', 'secondary_hover')
        ]

        for btn_type, color_key, hover_key in btn_styles:
            style.configure(f'{btn_type}.TButton', 
                          font=('Segoe UI', 10, 'bold'),
                          foreground='white', 
                          background=c[color_key], 
                          borderwidth=0,
                          focuscolor=c['bg2'],
                          padding=(15, 8))
            
            # Mappa dinamica per hover e active
            style.map(f'{btn_type}.TButton',
                     background=[('active', c[hover_key]), ('pressed', c[color_key])],
                     foreground=[('active', 'white'), ('pressed', 'white')])
        
        # LabelFrame moderno
        style.configure('Card.TLabelframe', background=c['bg2'], foreground=c['text'], 
                       borderwidth=1, relief='solid')
        style.configure('Card.TLabelframe.Label', font=('Segoe UI', 12, 'bold'),
                       foreground=c['text'], background=c['bg2'])
        
    def create_widgets(self):
        """Crea interfaccia moderna e compatta"""
        c = self.colors
        
        # Frame principale
        main = ttk.Frame(self.root, padding="15", style='Main.TFrame')
        main.grid(row=0, column=0, sticky="nsew")
        
        # Header con titolo e bottoni
        header = ttk.Frame(main, style='Header.TFrame', padding="15")
        header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 15))
        
        # Titolo
        title_box = ttk.Frame(header, style='Header.TFrame')
        title_box.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(title_box, text="🌐 EldenRing Torrent", style='Title.TLabel').pack(anchor=tk.W)
        ttk.Label(title_box, text="P2P Network Monitor", style='Subtitle.TLabel').pack(anchor=tk.W, pady=(2, 0))
        
        # Bottoni
        btn_box = ttk.Frame(header, style='Header.TFrame')
        btn_box.pack(side=tk.RIGHT, padx=(20, 0))
        
        buttons = [
            ("🌙 Theme", self.toggle_theme, 'Pink'),       # Pink for Theme
            ("🔄 Refresh", self.refresh_data, 'Secondary'), # Gray for Utility
            ("➕ Add", self.add_peer, 'Success'),          # Green for Create
            ("↩️ Join", self.join_existing_peer, 'Info'),  # Blue for Connect
            ("⬆️ Upload", self.upload_file, 'Indigo'),     # Indigo for Upload
            ("⬇️ Download", self.download_file, 'Teal'),   # Teal for Download
            ("🚪 Leave", self.leave_peer, 'Warning'),      # Orange for Leave
            ("🗑️ Delete", self.delete_peer, 'Danger')      # Red for Delete
        ]
        
        for text, cmd, style in buttons:
            btn = ttk.Button(btn_box, text=text, command=cmd, 
                      style=f'{style}.TButton', cursor='hand2')
            btn.pack(side=tk.LEFT, padx=4)
        
        # Info bar
        info_frame = ttk.Frame(main, style='Header.TFrame', padding="10")
        info_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 10))
        self.info_label = ttk.Label(info_frame, text="", style='Status.TLabel')
        self.info_label.pack(side=tk.LEFT)
        
        # Lista peers (sinistra)
        left = ttk.LabelFrame(main, text="📂 Peers", padding="10", style='Card.TLabelframe')
        left.grid(row=2, column=0, sticky="nsew", padx=(0, 10))
        
        self.peer_listbox = tk.Listbox(left, font=('Consolas', 10), selectmode=tk.SINGLE,
                                       bg=c['bg3'], fg=c['text'], selectbackground=c['accent'],
                                       selectforeground='white', relief=tk.FLAT, 
                                       highlightthickness=0, borderwidth=0)
        self.peer_listbox.pack(fill=tk.BOTH, expand=True)
        self.peer_listbox.bind('<<ListboxSelect>>', self.on_peer_select)
        
        # Dettagli peer (destra)
        right = ttk.LabelFrame(main, text="📊 Details", padding="10", style='Card.TLabelframe')
        right.grid(row=2, column=1, sticky="nsew")
        
        self.details_text = scrolledtext.ScrolledText(right, font=('Consolas', 9), wrap=tk.WORD,
                                                      bg=c['bg3'], fg=c['text'], relief=tk.FLAT,
                                                      highlightthickness=0, insertbackground=c['accent'],
                                                      padx=10, pady=10)
        self.details_text.pack(fill=tk.BOTH, expand=True)
        
        # Tag per formattazione testo
        tags = {
            'header': (c['accent'], 'bold', 12),
            'section': (c['text'], 'bold', 10),
            'key': (c['accent'], 'bold', 9),
            'value': (c['text'], 'normal', 9),
            'success': (c['success'], 'normal', 9),
            'warning': (c['warning'], 'normal', 9),
            'error': (c['error'], 'normal', 9)
        }
        
        for tag, (fg, weight, size) in tags.items():
            self.details_text.tag_config(tag, foreground=fg, 
                                        font=('Consolas', size, weight if weight == 'bold' else ''))
        
        # Configurazione griglia responsive
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main.columnconfigure(0, weight=1, minsize=300)
        main.columnconfigure(1, weight=2, minsize=600)
        main.rowconfigure(2, weight=1)
        
    def scan_peers(self):
        """Scansiona cartelle data_peer* e raccoglie informazioni"""
        peers_data = {}
        
        for peer_dir in sorted(self.base_dir.glob('data_peer*')):
            if not peer_dir.is_dir():
                continue
                
            peer_info = {
                'chunks': defaultdict(list), 
                'manifests': [], 
                'files': [],
                'unknown': []
            }
            
            # 1. Raccogli tutti i manifest e i loro chunk hash
            known_chunk_hashes = set()
            manifest_files = set()
            
            # Prima passata: Manifests
            for file_path in peer_dir.glob('*.manifest.json'):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.loads(f.read().strip())
                        manifest_hash = file_path.name.replace('.manifest.json', '')
                        peer_info['manifests'].append({'hash': file_path.name, 'data': data})
                        manifest_files.add(file_path.name)
                        
                        for chunk_info in data.get('chunks', []):
                            chunk_hash = chunk_info.get('hash') if isinstance(chunk_info, dict) else chunk_info
                            if chunk_hash:
                                known_chunk_hashes.add(chunk_hash)
                except Exception as e:
                    peer_info['unknown'].append({'hash': file_path.name, 'error': str(e)})

            # 2. Seconda passata: File e Chunks
            for file_path in peer_dir.iterdir():
                if not file_path.is_file():
                    continue
                    
                filename = file_path.name
                if filename in manifest_files:
                    continue
                    
                if filename in known_chunk_hashes:
                    # È un chunk conosciuto
                    found = False
                    for m in peer_info['manifests']:
                        m_data = m['data']
                        m_chunks = [c.get('hash') if isinstance(c, dict) else c for c in m_data.get('chunks', [])]
                        if filename in m_chunks:
                            peer_info['chunks'][m['hash'].replace('.manifest.json', '')].append({
                                'hash': filename,
                                'file_name': m_data.get('filename', 'Unknown')
                            })
                            found = True
                            break
                    if not found:
                        peer_info['chunks']['orphan'].append({'hash': filename})
                else:
                    # Non è un manifest, non è un chunk conosciuto.
                    # Heuristic: Se sembra un hash SHA256 (64 hex chars), è un orphan chunk
                    if len(filename) == 64 and all(c in string.hexdigits for c in filename):
                        peer_info['chunks']['orphan'].append({'hash': filename})
                    else:
                        # È un file intero
                        peer_info['files'].append(filename)
            
            peers_data[peer_dir.name] = peer_info
        
        return peers_data
    
    def refresh_data(self):
        """Aggiorna dati scansionando le cartelle"""
        self.peers_data = self.scan_peers()
        self.update_peer_list()
        
        total_peers = len(self.peers_data)
        total_manifests = sum(len(p['manifests']) for p in self.peers_data.values())
        total_chunks = sum(sum(len(chunks) for chunks in p['chunks'].values()) 
                          for p in self.peers_data.values())
        total_files = sum(len(p.get('files', [])) for p in self.peers_data.values())
        
        self.info_label.config(text=f"🌐 Peers: {total_peers} | 📄 Manifests: {total_manifests} | 📦 Chunks: {total_chunks} | 📁 Files: {total_files}")

    def toggle_theme(self):
        """Cambia tra tema scuro e chiaro"""
        self.dark_mode = not self.dark_mode
        self.colors = self.THEMES['dark' if self.dark_mode else 'light']
        
        # Ricrea interfaccia
        for widget in self.root.winfo_children():
            widget.destroy()
        
        self.setup_styles()
        self.create_widgets()
        self.refresh_data()

    def get_peer_status(self, peer_name):
        """Restituisce icona stato del peer"""
        container_name = peer_name.replace("data_", "")
        running = self.get_running_containers()
        all_containers = self.get_all_containers()
        
        if container_name in running:
            return "● "  # Verde - Online
        elif container_name in all_containers:
            return "● "  # Giallo - Stopped
        return "○ "  # Bianco - Not created

    def get_running_containers(self):
        """Ottiene container Docker in esecuzione"""
        try:
            result = subprocess.check_output(
                ["docker", "ps", "--format", "{{.Names}}"],
                stderr=subprocess.STDOUT
            ).decode().splitlines()
            return set(result)
        except Exception:
            return set()

    def get_all_containers(self):
        """Ottiene tutti i container Docker"""
        try:
            result = subprocess.check_output(
                ["docker", "ps", "-a", "--format", "{{.Names}}"],
                stderr=subprocess.STDOUT
            ).decode().splitlines()
            return set(result)
        except Exception:
            return set()

    def add_peer(self):
        """Crea nuovo peer e lo joina alla rete"""
        try:
            existing_peers = [int(p.name.replace("data_peer", "")) 
                            for p in self.base_dir.glob("data_peer*") 
                            if p.name.replace("data_peer", "").isdigit()]
            next_id = max(existing_peers, default=0) + 1
            new_peer_data = self.base_dir / f"data_peer{next_id}"
            new_peer_data.mkdir(exist_ok=True)

            peer_name = f"peer{next_id}"
            port = 5000 + next_id
            
            cmd = [
                "docker", "run", "-d", "--name", peer_name,
                "--network", "eldenringtorrent-_p2p_net",
                "-p", f"{port}:5000",
                "-v", f"{str(new_peer_data)}:/app/data",
                "-e", "PORT=5000", "-e", "DATA_DIR=/app/data",
                "-e", f"SELF_ID={peer_name}:5000",
                "-e", "KNOWN_PEERS=peer1:5000,peer2:5000,peer3:5000,peer4:5000,peer5:5000,peer6:5000,peer7:5000",
                "eldenringtorrent--peer1"
            ]
            subprocess.check_call(cmd, stderr=subprocess.STDOUT)

            messagebox.showinfo("Success", f"✅ Peer {peer_name} creato!\nPorta: {port}")
            self.info_label.config(text=f"🟢 {peer_name} aggiunto")
            sleep(1)
            self.refresh_data()
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")
            self.info_label.config(text=f"❌ Errore creazione peer")

    def join_existing_peer(self):
        """Rejoin peer esistente con dati"""
        try:
            existing_data_dirs = [p.name.replace("data_peer", "") 
                                 for p in self.base_dir.glob("data_peer*") 
                                 if p.name.replace("data_peer", "").isdigit()]
            
            running = self.get_running_containers()
            stopped_peers = [(num, f"peer{num}") for num in existing_data_dirs 
                           if f"peer{num}" not in running]
            
            if not stopped_peers:
                messagebox.showinfo("Info", "⚠️ Nessun peer da rejoinare")
                return
            
            # Dialog selezione peer
            dialog = tk.Toplevel(self.root)
            dialog.title("Join Peer")
            dialog.geometry("450x350")
            dialog.configure(bg=self.colors['bg'])
            
            header = ttk.Frame(dialog, style='Header.TFrame', padding="15")
            header.pack(fill=tk.X)
            ttk.Label(header, text="🔄 Seleziona Peer", style='Title.TLabel').pack(anchor=tk.W)
            
            listbox = tk.Listbox(dialog, font=('Consolas', 10),
                               bg=self.colors['bg3'], fg=self.colors['text'],
                               selectbackground=self.colors['accent'])
            listbox.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)
            
            for peer_num, peer_name in stopped_peers:
                data_dir = self.base_dir / f"data_peer{peer_num}"
                num_files = len(list(data_dir.glob("*"))) if data_dir.exists() else 0
                listbox.insert(tk.END, f"🟡 {peer_name:<10} │ {num_files} files")
            
            def do_join():
                sel = listbox.curselection()
                if not sel:
                    messagebox.showwarning("Warning", "Seleziona un peer")
                    return
                peer_num, peer_name = stopped_peers[sel[0]]
                dialog.destroy()
                self._start_peer_container(peer_num, peer_name)
            
            btn_frame = ttk.Frame(dialog, style='Header.TFrame', padding="15")
            btn_frame.pack(fill=tk.X)
            ttk.Button(btn_frame, text="✅ Join", command=do_join, 
                      style='Success.TButton').pack(side=tk.LEFT, padx=5)
            ttk.Button(btn_frame, text="❌ Annulla", command=dialog.destroy,
                      style='Modern.TButton').pack(side=tk.LEFT)
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")

    def _start_peer_container(self, peer_num, peer_name):
        """Avvia container peer"""
        try:
            peer_data = self.base_dir / f"data_peer{peer_num}"
            port = 5000 + int(peer_num)
            
            if peer_name in self.get_all_containers():
                subprocess.check_call(["docker", "start", peer_name])
                messagebox.showinfo("Success", f"✅ {peer_name} riavviato!")
            else:
                cmd = [
                    "docker", "run", "-d", "--name", peer_name,
                    "--network", "eldenringtorrent-_p2p_net",
                    "-p", f"{port}:5000",
                    "-v", f"{str(peer_data)}:/app/data",
                    "-e", "PORT=5000", "-e", "DATA_DIR=/app/data",
                    "-e", f"SELF_ID={peer_name}:5000",
                    "-e", "KNOWN_PEERS=peer1:5000,peer2:5000,peer3:5000,peer4:5000,peer5:5000,peer6:5000,peer7:5000",
                    "eldenringtorrent--peer1"
                ]
                subprocess.check_call(cmd)
                messagebox.showinfo("Success", f"✅ {peer_name} rejoinato!")
            
            self.info_label.config(text=f"🟢 {peer_name} online")
            sleep(1)
            self.refresh_data()
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")

    def leave_peer(self):
        """Peer esce dalla rete (container fermato)"""
        try:
            sel = self.peer_listbox.curselection()
            if not sel:
                messagebox.showwarning("Warning", "⚠️ Seleziona un peer")
                return

            peer_name = sorted(self.peers_data.keys())[sel[0]]
            container_name = peer_name.replace("data_", "")

            if container_name not in self.get_running_containers():
                messagebox.showinfo("Info", f"⚠️ {container_name} non è attivo")
                return

            # Graceful shutdown
            try:
                peer_num = container_name.replace("peer", "")
                port = 5000 + int(peer_num)
                requests.post(f"http://localhost:{port}/leave",
                            json={"peer_id": f"{container_name}:5000"}, timeout=10)
            except Exception:
                pass

            subprocess.check_call(["docker", "stop", container_name])
            messagebox.showinfo("Success", f"✅ {container_name} offline")
            self.info_label.config(text=f"🚪 {container_name} in leave")
            sleep(1)
            self.refresh_data()
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")

    def delete_peer(self):
        """Elimina completamente peer"""
        try:
            sel = self.peer_listbox.curselection()
            if not sel:
                messagebox.showwarning("Warning", "⚠️ Seleziona un peer")
                return

            peer_name = sorted(self.peers_data.keys())[sel[0]]
            container_name = peer_name.replace("data_", "")
            data_dir = self.base_dir / peer_name

            if not messagebox.askyesno("Conferma",
                f"⚠️ Eliminare {container_name}?\nQuesta operazione è IRREVERSIBILE!"):
                return

            # Graceful shutdown
            try:
                peer_num = container_name.replace("peer", "")
                port = 5000 + int(peer_num)
                requests.post(f"http://localhost:{port}/leave",
                            json={"peer_id": f"{container_name}:5000"}, timeout=10)
            except Exception:
                pass

            subprocess.run(["docker", "stop", container_name], check=False, stderr=subprocess.DEVNULL)
            subprocess.run(["docker", "rm", container_name], check=False, stderr=subprocess.DEVNULL)

            if data_dir.exists():
                shutil.rmtree(data_dir)

            messagebox.showinfo("Success", f"✅ {container_name} eliminato!")
            self.info_label.config(text=f"🗑️ {container_name} eliminato")
            sleep(1)
            self.refresh_data()
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")

    def upload_file(self):
        """Upload file su un peer selezionato"""
        try:
            sel = self.peer_listbox.curselection()
            if not sel:
                messagebox.showwarning("Warning", "⚠️ Seleziona prima un peer")
                return

            peer_name = sorted(self.peers_data.keys())[sel[0]]
            container_name = peer_name.replace("data_", "")

            # Verifica se il peer è attivo
            if container_name not in self.get_running_containers():
                messagebox.showwarning("Warning", f"⚠️ {container_name} non è attivo!\nAvvialo prima di fare upload.")
                return

            # Dialog per selezione file
            from tkinter import filedialog
            file_path = filedialog.askopenfilename(
                title="Seleziona file da uploadare",
                initialdir=str(self.base_dir)
            )
            
            if not file_path:
                return
            
            # Ottieni la porta del peer
            peer_num = container_name.replace("peer", "")
            port = 5000 + int(peer_num)
            
            # Determina il percorso del file nel container
            file_path_obj = Path(file_path)
            data_dir = self.base_dir / peer_name
            dest_path = data_dir / file_path_obj.name
            
            # Se il file è già nella cartella del peer, usa direttamente il percorso
            if file_path_obj.resolve() == dest_path.resolve():
                container_path = f"/app/data/{file_path_obj.name}"
            else:
                # Altrimenti, copia il file nella cartella del peer (se non esiste già)
                if not dest_path.exists():
                    import shutil
                    shutil.copy2(file_path, dest_path)
                    messagebox.showinfo("Info", f"📂 File copiato in {peer_name}")
                container_path = f"/app/data/{file_path_obj.name}"
            
            # Chiamata API per store_file
            url = f"http://localhost:{port}/store_file"
            payload = {"filename": container_path}
            
            self.info_label.config(text=f"📤 Uploading {file_path_obj.name}...")
            self.root.update()
            
            response = requests.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                messagebox.showinfo("Success", 
                    f"✅ File uploadato con successo!\n\n"
                    f"Peer: {container_name}\n"
                    f"File: {file_path_obj.name}\n"
                    f"Manifest Hash: {result.get('manifest_hash', 'N/A')[:40]}...")
                self.info_label.config(text=f"✅ Upload completato su {container_name}")
            else:
                messagebox.showerror("Error", 
                    f"❌ Errore upload!\n\n"
                    f"Status: {response.status_code}\n"
                    f"Risposta: {response.text[:200]}")
                self.info_label.config(text=f"❌ Errore upload")
            
            sleep(1)
            self.refresh_data()
            
        except requests.exceptions.Timeout:
            messagebox.showerror("Error", "❌ Timeout: il peer non risponde")
            self.info_label.config(text=f"❌ Timeout upload")
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")
            self.info_label.config(text=f"❌ Errore upload")

    def download_file(self):
        """Download file da un peer selezionato"""
        try:
            sel = self.peer_listbox.curselection()
            if not sel:
                messagebox.showwarning("Warning", "⚠️ Seleziona prima un peer")
                return

            peer_name = sorted(self.peers_data.keys())[sel[0]]
            container_name = peer_name.replace("data_", "")

            # Verifica se il peer è attivo
            if container_name not in self.get_running_containers():
                messagebox.showwarning("Warning", f"⚠️ {container_name} non è attivo!\nAvvialo prima di fare download.")
                return

            peer_info = self.peers_data[peer_name]
            
            # Verifica se ci sono manifest disponibili
            if not peer_info['manifests']:
                messagebox.showinfo("Info", f"⚠️ {peer_name} non ha file disponibili per il download")
                return

            # Dialog per selezione file da scaricare
            dialog = tk.Toplevel(self.root)
            dialog.title(f"Download da {container_name}")
            dialog.geometry("600x400")
            dialog.configure(bg=self.colors['bg'])
            
            header = ttk.Frame(dialog, style='Header.TFrame', padding="15")
            header.pack(fill=tk.X)
            ttk.Label(header, text=f"📥 Seleziona File da {container_name}", 
                     style='Title.TLabel').pack(anchor=tk.W)
            
            # Lista file disponibili
            listbox = tk.Listbox(dialog, font=('Consolas', 10),
                               bg=self.colors['bg3'], fg=self.colors['text'],
                               selectbackground=self.colors['accent'])
            listbox.pack(fill=tk.BOTH, expand=True, padx=15, pady=10)
            
            # Popola lista con i file disponibili nella rete
            available_files = {}
            for manifest_info in peer_info['manifests']:
                filename = manifest_info['data'].get('filename', 'Unknown')
                num_chunks = len(manifest_info['data'].get('chunks', []))
                display_name = Path(filename).name if filename != 'Unknown' else manifest_info['hash'][:20]
                listbox.insert(tk.END, f"📄 {display_name:<40} │ {num_chunks} chunks")
                available_files[listbox.size() - 1] = display_name
            
            def do_download():
                sel_idx = listbox.curselection()
                if not sel_idx:
                    messagebox.showwarning("Warning", "Seleziona un file")
                    return
                
                filename = available_files[sel_idx[0]]
                dialog.destroy()
                self._perform_download(container_name, filename)
            
            btn_frame = ttk.Frame(dialog, style='Header.TFrame', padding="15")
            btn_frame.pack(fill=tk.X)
            ttk.Button(btn_frame, text="✅ Download", command=do_download, 
                      style='Success.TButton').pack(side=tk.LEFT, padx=5)
            ttk.Button(btn_frame, text="❌ Annulla", command=dialog.destroy,
                      style='Modern.TButton').pack(side=tk.LEFT)
            
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")

    def _perform_download(self, container_name, filename):
        """Esegue il download effettivo del file"""
        try:
            peer_num = container_name.replace("peer", "")
            port = 5000 + int(peer_num)
            
            # Chiamata API per fetch_file
            url = f"http://localhost:{port}/fetch_file"
            payload = {"filename": filename}
            
            self.info_label.config(text=f"📥 Downloading {filename}...")
            self.root.update()
            
            response = requests.post(url, json=payload, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                
                # Costruisci il percorso del file scaricato
                peer_data_dir = self.base_dir / f"data_{container_name}"
                downloaded_file = peer_data_dir / f"rebuilt_{filename}"
                
                if downloaded_file.exists():
                    messagebox.showinfo("Success", 
                        f"✅ File scaricato con successo!\n\n"
                        f"Peer: {container_name}\n"
                        f"File: {filename}\n"
                        f"Percorso: {downloaded_file}\n"
                        f"Status: {result.get('status', 'N/A')}")
                else:
                    messagebox.showinfo("Info", 
                        f"✅ Download completato!\n\n"
                        f"Peer: {container_name}\n"
                        f"File: {filename}\n"
                        f"Risposta: {result}")
                
                self.info_label.config(text=f"✅ Download completato: {filename}")
            else:
                messagebox.showerror("Error", 
                    f"❌ Errore download!\n\n"
                    f"Status: {response.status_code}\n"
                    f"Risposta: {response.text[:200]}")
                self.info_label.config(text=f"❌ Errore download")
            
            sleep(1)
            self.refresh_data()
            
        except requests.exceptions.Timeout:
            messagebox.showerror("Error", "❌ Timeout: il download sta richiedendo troppo tempo")
            self.info_label.config(text=f"❌ Timeout download")
        except Exception as e:
            messagebox.showerror("Error", f"❌ Errore: {e}")
            self.info_label.config(text=f"❌ Errore download")

    def update_peer_list(self):
        """Aggiorna lista peer"""
        self.peer_listbox.delete(0, tk.END)
        
        running = self.get_running_containers()
        all_containers = self.get_all_containers()
        
        for peer_name in sorted(self.peers_data.keys()):
            peer_info = self.peers_data[peer_name]
            num_manifests = len(peer_info['manifests'])
            num_chunks = sum(len(chunks) for chunks in peer_info['chunks'].values())
            num_files = len(peer_info.get('files', []))
            
            container_name = peer_name.replace("data_", "")
            
            # Determina lo stato e il simbolo
            if container_name in running:
                status_symbol = "●"  # Pieno - Online
                status_text = "ON "
            elif container_name in all_containers:
                status_symbol = "○"  # Vuoto - Offline
                status_text = "OFF"
            else:
                status_symbol = "－"  # Linea - Not created
                status_text = "N/A"
            
            display_text = f"{status_symbol} {status_text} {peer_name:<12} │ 📄 {num_manifests:>2} │ 📦 {num_chunks:>3} │ 📁 {num_files:>2}"
            self.peer_listbox.insert(tk.END, display_text)
    
    def on_peer_select(self, event):
        """Gestisce selezione peer"""
        sel = self.peer_listbox.curselection()
        if sel:
            peer_name = sorted(self.peers_data.keys())[sel[0]]
            self.display_peer_details(peer_name)
    
    def display_peer_details(self, peer_name):
        """Visualizza dettagli peer con layout compatto"""
        self.details_text.delete('1.0', tk.END)
        peer_info = self.peers_data[peer_name]
        container_name = peer_name.replace("data_", "")
        
        # Header
        running = self.get_running_containers()
        status = "ONLINE" if container_name in running else ("OFFLINE" if container_name in self.get_all_containers() else "NOT CREATED")
        
        self.details_text.insert(tk.END, f"═══ {peer_name.upper()} ═══\n", 'header')
        self.details_text.insert(tk.END, f"Status: {status}\n\n", 'section')
        
        # Whole Files
        if peer_info.get('files'):
            self.details_text.insert(tk.END, f"📂 WHOLE FILES ({len(peer_info['files'])})\n", 'section')
            for f in peer_info['files']:
                self.details_text.insert(tk.END, f"  • {f}\n", 'info')
            self.details_text.insert(tk.END, "\n", 'value')

        # Manifests
        if peer_info['manifests']:
            self.details_text.insert(tk.END, f"📋 MANIFESTS ({len(peer_info['manifests'])})\n", 'section')
            for idx, m in enumerate(peer_info['manifests'][:3], 1):
                self.details_text.insert(tk.END, f"  [{idx}] ", 'key')
                self.details_text.insert(tk.END, f"{m['data'].get('filename', 'N/A')}\n", 'value')
                self.details_text.insert(tk.END, f"      Hash: {m['hash'][:40]}...\n", 'value')
                self.details_text.insert(tk.END, f"      Chunks: {len(m['data'].get('chunks', []))}\n\n", 'success')
            if len(peer_info['manifests']) > 3:
                self.details_text.insert(tk.END, f"  ... and {len(peer_info['manifests'])-3} more\n\n", 'value')
        
        # Chunks
        self.details_text.insert(tk.END, "\n📦 CHUNKS\n", 'section')
        for file_hash, chunks in list(peer_info['chunks'].items())[:5]:
            if not chunks or file_hash == 'orphan':
                continue
            file_name = chunks[0].get('file_name', 'Unknown')
            self.details_text.insert(tk.END, f"  • {file_name}: ", 'key')
            self.details_text.insert(tk.END, f"{len(chunks)} chunks\n", 'success')
        
        if peer_info['chunks'].get('orphan'):
            self.details_text.insert(tk.END, f"\n  ⚠️ Orphan chunks: {len(peer_info['chunks']['orphan'])}\n", 'warning')
        
        # Unknown
        if peer_info['unknown']:
            self.details_text.insert(tk.END, f"\n⚠️ UNKNOWN FILES ({len(peer_info['unknown'])})\n", 'error')


def main():
    root = tk.Tk()
    app = PeerMonitorGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
