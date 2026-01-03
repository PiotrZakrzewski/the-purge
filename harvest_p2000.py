import urllib.request
import re
import time
import sys
import signal
import csv
import argparse
from datetime import datetime

# Global control flag
keep_running = True

def signal_handler(sig, frame):
    """Handle Ctrl+C to stop the loop gracefully."""
    global keep_running
    print("\nCtrl+C detected! Finishing current page and saving data...")
    keep_running = False

def harvest_p2000(max_page, output_file):
    global keep_running
    
    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    base_url = "https://www.p2000-online.net/p2000.py"
    
    # Regex to parse the row (reused from original)
    # <td class="DT">01-01-2026 22:43:11</td><td class="Br">Brandweer</td><td class="Regio">Brabant Noord</td><td class="Mdx">...</td>
    row_pattern = re.compile(r'<td class="DT">(\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2})</td><td class=".*?">(.*?)</td><td class="Regio">(.*?)</td><td class=".*?">(.*?)</td>')
    
    print(f"Starting harvest. Target: Pages 1 to {max_page}")
    print(f"Saving to: {output_file}")
    print("Press Ctrl+C to stop safely.")

    current_page = 1
    total_records = 0

    try:
        # Open file in write mode (creates new file)
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            # Write Header
            writer.writerow(["Timestamp", "Service", "Region", "Message"])
            
            while keep_running and current_page <= max_page:
                url = f"{base_url}?aantal=50&pagina={current_page}"
                sys.stdout.write(f"\rFetching page {current_page}/{max_page} (Total records: {total_records})...")
                sys.stdout.flush()
                
                try:
                    with urllib.request.urlopen(url) as response:
                        html = response.read().decode('utf-8', errors='ignore')
                except Exception as e:
                    print(f"\nError fetching page {current_page}: {e}")
                    # Sleep a bit longer on error
                    time.sleep(2)
                    current_page += 1 
                    continue
                    
                matches = row_pattern.findall(html)
                
                if not matches:
                    print(f"\nNo matches found on page {current_page}. Stopping.")
                    break
                    
                for dt_str, service, region, message in matches:
                    writer.writerow([dt_str, service, region, message])
                    total_records += 1
                
                # Flush to disk to ensure data is safe
                f.flush()
                
                current_page += 1
                time.sleep(0.5) # Be polite to the server

    except IOError as e:
        print(f"\nFile error: {e}")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    finally:
        print(f"\n\nHarvest stopped. Saved {total_records} records to {output_file}")

if __name__ == "__main__":
    # Generate default filename with actual datetime and .tsv extension
    now_str = datetime.now().strftime("%Y-%m-%d-%H:%M")
    default_filename = f"p2000_{now_str}.tsv"
    
    parser = argparse.ArgumentParser(description="Harvest P2000 data continuously to a TSV file.")
    parser.add_argument("--maxPage", type=int, default=1000, help="Maximum number of pages to scrape (default: 1000)")
    parser.add_argument("--output", type=str, default=default_filename, help=f"Output file name (default: {default_filename})")
    
    args = parser.parse_args()
    
    harvest_p2000(args.maxPage, args.output)
