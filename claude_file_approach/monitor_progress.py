#!/usr/bin/env python
"""
Monitor progress of the extraction and ingestion process.

This script provides real-time monitoring of:
1. Extraction progress (parquet file generation)
2. Disk space usage
3. Processing speed estimates
4. Database statistics
"""
import os
import sys
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import argparse

import duckdb
import humanize
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel

console = Console()

def get_directory_size(path):
    """Get total size of a directory."""
    total = 0
    for entry in Path(path).rglob('*'):
        if entry.is_file():
            total += entry.stat().st_size
    return total

def get_parquet_stats(parquet_dir):
    """Get statistics about parquet files."""
    stats = {}
    
    if not Path(parquet_dir).exists():
        return stats
    
    for table_dir in Path(parquet_dir).iterdir():
        if table_dir.is_dir():
            parquet_files = list(table_dir.rglob("*.parquet"))
            total_size = sum(f.stat().st_size for f in parquet_files)
            
            stats[table_dir.name] = {
                'file_count': len(parquet_files),
                'total_size': total_size,
                'avg_size': total_size / len(parquet_files) if parquet_files else 0
            }
    
    return stats

def get_extraction_progress(checkpoint_file='extraction_checkpoint.json'):
    """Get extraction progress from checkpoint."""
    if not os.path.exists(checkpoint_file):
        return None
    
    try:
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
            return {
                'processed': len(data.get('processed', [])),
                'last_update': data.get('timestamp')
            }
    except:
        return None

def get_database_stats(db_path):
    """Get database statistics."""
    if not Path(db_path).exists():
        return None
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        stats = {}
        tables = [
            ('functional_profile.profiles', 'functional_profiles'),
            ('taxa_profiles.profiles', 'taxa_profiles'),
            ('functional_profile_data.gather_data', 'gather_data'),
            ('sigs_aa.signatures', 'sigs_aa'),
            ('sigs_dna.signatures', 'sigs_dna')
        ]
        
        for table, name in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                stats[name] = count
            except:
                stats[name] = 0
        
        # Get database size
        try:
            db_size = conn.execute("SELECT SUM(bytes) FROM duckdb_blocks()").fetchone()[0]
            stats['db_size'] = db_size or 0
        except:
            stats['db_size'] = 0
        
        conn.close()
        return stats
        
    except Exception as e:
        return None

def estimate_completion_time(processed, total, start_time):
    """Estimate completion time based on current progress."""
    if processed == 0:
        return "Unknown"
    
    elapsed = datetime.now() - start_time
    rate = processed / elapsed.total_seconds()
    remaining = total - processed
    
    if rate > 0:
        eta_seconds = remaining / rate
        eta = datetime.now() + timedelta(seconds=eta_seconds)
        return eta.strftime("%Y-%m-%d %H:%M:%S")
    
    return "Unknown"

def create_progress_table(data):
    """Create a rich table for progress display."""
    table = Table(title="Extraction Progress", show_header=True)
    
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    for key, value in data.items():
        table.add_row(key, str(value))
    
    return table

def create_parquet_table(stats):
    """Create a table for parquet statistics."""
    table = Table(title="Parquet Files", show_header=True)
    
    table.add_column("Table", style="cyan")
    table.add_column("Files", justify="right", style="green")
    table.add_column("Total Size", justify="right", style="yellow")
    table.add_column("Avg Size", justify="right", style="blue")
    
    total_files = 0
    total_size = 0
    
    for table_name, info in sorted(stats.items()):
        table.add_row(
            table_name,
            f"{info['file_count']:,}",
            humanize.naturalsize(info['total_size']),
            humanize.naturalsize(info['avg_size'])
        )
        total_files += info['file_count']
        total_size += info['total_size']
    
    table.add_row(
        "[bold]Total[/bold]",
        f"[bold]{total_files:,}[/bold]",
        f"[bold]{humanize.naturalsize(total_size)}[/bold]",
        ""
    )
    
    return table

def create_database_table(stats):
    """Create a table for database statistics."""
    if not stats:
        return Panel("Database not yet created", title="Database Statistics")
    
    table = Table(title="Database Statistics", show_header=True)
    
    table.add_column("Table", style="cyan")
    table.add_column("Row Count", justify="right", style="green")
    
    total_rows = 0
    
    for table_name, count in sorted(stats.items()):
        if table_name != 'db_size':
            table.add_row(table_name, f"{count:,}")
            total_rows += count
    
    table.add_row("[bold]Total Rows[/bold]", f"[bold]{total_rows:,}[/bold]")
    
    if 'db_size' in stats and stats['db_size'] > 0:
        table.add_row(
            "[bold]Database Size[/bold]",
            f"[bold]{humanize.naturalsize(stats['db_size'])}[/bold]"
        )
    
    return table

def monitor_loop(args):
    """Main monitoring loop."""
    start_time = datetime.now()
    
    with Live(console=console, refresh_per_second=1) as live:
        while True:
            try:
                # Get current statistics
                extraction_progress = get_extraction_progress()
                parquet_stats = get_parquet_stats(args.parquet_dir)
                db_stats = get_database_stats(args.database)
                
                # Get disk space
                import shutil
                disk_usage = shutil.disk_usage(args.parquet_dir)
                free_space = humanize.naturalsize(disk_usage.free)
                used_space = humanize.naturalsize(disk_usage.used)
                
                # Create layout
                layout = Layout()
                layout.split_column(
                    Layout(name="header", size=3),
                    Layout(name="body"),
                    Layout(name="footer", size=3)
                )
                
                # Header
                layout["header"].update(
                    Panel(
                        f"[bold cyan]Logan Data Processing Monitor[/bold cyan]\n"
                        f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')} | "
                        f"Running: {humanize.naturaldelta(datetime.now() - start_time)}",
                        style="on blue"
                    )
                )
                
                # Body
                body_layout = Layout()
                body_layout.split_row(
                    Layout(name="left"),
                    Layout(name="right")
                )
                
                # Left side - extraction progress
                if extraction_progress:
                    progress_data = {
                        "Archives Processed": f"{extraction_progress['processed']:,} / {args.total_archives:,}",
                        "Progress": f"{extraction_progress['processed'] / args.total_archives * 100:.1f}%",
                        "ETA": estimate_completion_time(
                            extraction_progress['processed'],
                            args.total_archives,
                            start_time
                        ),
                        "Last Update": extraction_progress['last_update']
                    }
                    left_content = create_progress_table(progress_data)
                else:
                    left_content = Panel("Extraction not started", title="Extraction Progress")
                
                body_layout["left"].split_column(
                    Layout(left_content),
                    Layout(create_parquet_table(parquet_stats))
                )
                
                # Right side - database stats and disk space
                disk_info = Table(title="Disk Space", show_header=True)
                disk_info.add_column("Metric", style="cyan")
                disk_info.add_column("Value", style="green")
                disk_info.add_row("Free Space", free_space)
                disk_info.add_row("Used Space", used_space)
                
                body_layout["right"].split_column(
                    Layout(create_database_table(db_stats)),
                    Layout(disk_info)
                )
                
                layout["body"].update(body_layout)
                
                # Footer
                layout["footer"].update(
                    Panel(
                        "[dim]Press Ctrl+C to exit | Updates every 5 seconds[/dim]",
                        style="dim"
                    )
                )
                
                live.update(layout)
                time.sleep(5)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                console.print(f"[red]Error: {e}[/red]")
                time.sleep(5)

def parse_args():
    parser = argparse.ArgumentParser(description='Monitor Logan data processing progress')
    
    parser.add_argument('--parquet-dir', default='./parquet_output',
                       help='Parquet output directory (default: ./parquet_output)')
    parser.add_argument('--database', default='functional_profile.db',
                       help='Database path (default: functional_profile.db)')
    parser.add_argument('--total-archives', type=int, default=1000,
                       help='Total number of archives to process (default: 1000)')
    
    return parser.parse_args()

def main():
    args = parse_args()
    
    console.print("[bold green]Starting Logan Data Processing Monitor[/bold green]")
    console.print(f"Monitoring parquet directory: {args.parquet_dir}")
    console.print(f"Database: {args.database}")
    console.print(f"Total archives: {args.total_archives}")
    console.print()
    
    try:
        monitor_loop(args)
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")

if __name__ == "__main__":
    main()