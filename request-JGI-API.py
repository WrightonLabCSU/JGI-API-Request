import argparse
import csv
import json
import requests
import subprocess

def fetch_json_data(taxon_oid):
    """Fetch JSON data for a given taxon_oid."""
    url = f"https://files.jgi.doe.gov/search/?q={taxon_oid}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {taxon_oid}")
        return {}
    
def parse_json_data(data):
    jamo_id = 'id-not-found'
    assembly_fasta_filename = ''
    assembly_fasta_file_id = ''
    assembly_fasta_file_size = 0
    assembly_fasta_file_status = ''
    raw_reads_filename = ''
    raw_reads_file_id = ''
    raw_reads_file_size = 0
    raw_reads_file_status = ''
    bins_fasta_filenames = []
    bins_fasta_file_ids = []
    bins_fasta_file_sizes = []
    bins_fasta_file_statuses = []

    organisms = data.get('organisms', [])
    for organism in organisms:
        top_hit = organism.get('top_hit')
        if top_hit:
            jamo_id = top_hit.get('_id', 'id-not-found')
        
        for file in organism.get('files', []):
            file_name = file.get('file_name', '')
            file_id = file.get('_id', '')
            file_size = file.get('file_size', 0)
            file_status = file.get('metadata', {}).get('data_utilization_status', 'Unknown')

            if 'scaffolds.fasta' in file_name:
                assembly_fasta_filename = file_name
                assembly_fasta_file_id = file_id
                assembly_fasta_file_size = file_size if file_size > 0 else assembly_fasta_file_size
                assembly_fasta_file_status = file_status
            
            if file_name.endswith('fastq.gz'):
                raw_reads_filename = file_name
                raw_reads_file_id = file_id
                raw_reads_file_size = file_size if file_size > 0 else raw_reads_file_size
                raw_reads_file_status = file_status
            
            if file.get('metadata', {}).get('content_type') == 'Binning Data' and file_name.endswith('.tar.gz'):
                bins_fasta_filenames.append(file_name)
                bins_fasta_file_ids.append(file_id)
                bins_fasta_file_sizes.append(file_size if file_size > 0 else 0)
                bins_fasta_file_statuses.append(file_status)

    bins_fasta_filenames_str = ";".join(bins_fasta_filenames)
    bins_fasta_file_ids_str = ";".join(bins_fasta_file_ids)
    bins_fasta_file_sizes_str = ";".join(map(str, bins_fasta_file_sizes))
    bins_fasta_file_statuses_str = ";".join(bins_fasta_file_statuses)
    
    return (jamo_id, assembly_fasta_filename, assembly_fasta_file_id, assembly_fasta_file_size, assembly_fasta_file_status,
            raw_reads_filename, raw_reads_file_id, raw_reads_file_size, raw_reads_file_status,
            bins_fasta_filenames_str, bins_fasta_file_ids_str, bins_fasta_file_sizes_str, bins_fasta_file_statuses_str)

def extract_additional_metadata(organism):
    additional_metadata = {
        'agg_id': organism.get('agg_id', ''),
        'kingdom': organism.get('kingdom', ''),
        'label': organism.get('label', ''),
        'country': organism['top_hit']['metadata']['proposal']['pi'].get('country', '') if 'top_hit' in organism and 'proposal' in organism['top_hit']['metadata'] else 'N/A',
        'institution': organism['top_hit']['metadata']['proposal']['pi'].get('institution', '') if 'top_hit' in organism and 'proposal' in organism['top_hit']['metadata'] else 'N/A',
        'its_sp_id': organism['top_hit']['metadata'].get('analysis_project', {}).get('sequencing_projects', [{}])[0].get('sequencing_project_id', 'N/A'),
        'its_ap_id': organism['top_hit']['metadata'].get('analysis_project_id', 'N/A')
    }
    return additional_metadata

def execute_curl_command(file_ids, token):
    file_ids_str = ','.join(f'"{fid}"' for fid in file_ids)
    curl_cmd = [
        'curl', '-X', 'POST', "https://files.jgi.doe.gov/request_archived_files/", 
        '-H', 'accept: application/json', '-H', f"Authorization: {token}", 
        '-H', 'Content-Type: application/json', 
        '-d', f'{{ "ids": [{file_ids_str}], "send_mail": true, "api_version": "2"}}'
    ]
    print("Running Curl command for pretend :D")
    #subprocess.run(curl_cmd)

def user_confirmation(prompt, required_response):
    response = input(prompt)
    return response.strip().upper() == required_response

def calculate_total_file_size(output_path):
    total_file_size = 0
    with open(output_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            total_file_size += int(row['assembly_fasta_file_size']) if row['assembly_fasta_file_size'].isdigit() else 0
            total_file_size += int(row['raw_reads_file_size']) if row['raw_reads_file_size'].isdigit() else 0
            bins_sizes = row['bins_fasta_file_size'].split(';')
            total_file_size += sum(int(size) for size in bins_sizes if size.isdigit())
    return total_file_size / (1024 ** 3)  # Convert bytes to GB

def extract_file_ids(output_path):
    """Extract file IDs from the output file for curl commands."""
    file_ids = []
    file_sizes = []  # You also need the sizes to split the file IDs accurately by size
    with open(output_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            # Assuming the file IDs are stored in 'assembly_fasta_file_id' and 'raw_reads_file_id'
            if row['assembly_fasta_file_id']:
                file_ids.append(row['assembly_fasta_file_id'])
                file_sizes.append(int(row['assembly_fasta_file_size']) if row['assembly_fasta_file_size'].isdigit() else 0)
            if row['raw_reads_file_id']:
                file_ids.append(row['raw_reads_file_id'])
                file_sizes.append(int(row['raw_reads_file_size']) if row['raw_reads_file_size'].isdigit() else 0)
            # Assuming bins file IDs are separated by semicolons like the sizes
            bins_ids = row['bins_fasta_file_id'].split(';')
            bins_sizes = row['bins_fasta_file_size'].split(';')
            for bin_id, bin_size in zip(bins_ids, bins_sizes):
                if bin_id and bin_size.isdigit():
                    file_ids.append(bin_id)
                    file_sizes.append(int(bin_size))

    return file_ids, file_sizes

def split_file_ids(file_ids, file_sizes, max_size_tb=10):
    """Split file IDs into batches, each batch having a total size < max_size_tb (in terabytes)."""
    batches = []
    current_batch = []
    current_batch_size = 0
    max_size_bytes = max_size_tb * 1024**4  # Convert TB to bytes

    for file_id, size in zip(file_ids, file_sizes):
        if current_batch_size + size > max_size_bytes:
            batches.append(current_batch)
            current_batch = [file_id]
            current_batch_size = size
        else:
            current_batch.append(file_id)
            current_batch_size += size

    if current_batch:
        batches.append(current_batch)
    
    return batches

def main(tsv_path, output_path, metadata_output_path, token):
    with open(tsv_path, 'r') as tsv_file, open(output_path, 'w', newline='') as out_file, open(metadata_output_path, 'w', newline='') as meta_out_file:
        reader = csv.reader(tsv_file, delimiter='\t')
        writer = csv.writer(out_file, delimiter='\t')
        meta_writer = csv.writer(meta_out_file, delimiter='\t')

        # Write headers
        output_headers = [
            'taxon_oid', 'jamo_id',
            'assembly_fasta_filename', 'assembly_fasta_file_id', 'assembly_fasta_file_size', 'assembly_fasta_file_status',
            'raw_reads_filename', 'raw_reads_file_id', 'raw_reads_file_size', 'raw_reads_file_status',
            'bins_fasta_filename', 'bins_fasta_file_id', 'bins_fasta_file_size', 'bins_fasta_file_status'
        ]
        meta_headers = ['taxon_oid', 'JAMO id', 'agg_id', 'kingdom', 'label', 'country', 'institution', 'its_sp_id', 'its_ap_id']
        writer.writerow(output_headers)
        meta_writer.writerow(meta_headers)

        next(reader, None)  # Skip header row

        for row in reader:
            taxon_oid = row[0]
            data = fetch_json_data(taxon_oid)
            parsed_data = parse_json_data(data)
            additional_metadata = extract_additional_metadata(data['organisms'][0]) if data.get('organisms') else {}
            
            writer.writerow([taxon_oid] + list(parsed_data))
            meta_writer.writerow([taxon_oid] + [parsed_data[0]] + list(additional_metadata.values()))

    # After processing all rows
    total_file_size_gb = calculate_total_file_size(output_path)
    file_ids, file_sizes = extract_file_ids(output_path)  # Fetch file IDs and sizes

    # Check for large file set
    if total_file_size_gb > 10000:  # More than 10 TB
        print(f"Warning: The requested data set is very large ({total_file_size_gb:.2f} GB).")
        if user_confirmation("Do you want to split the download into multiple smaller requests? (yes/no): ", "YES"):
            batches = split_file_ids(file_ids, file_sizes, max_size_tb=10)
            for batch in batches:
                if user_confirmation(f"Proceed with downloading a batch of {len(batch)} files? (yes/no): ", "YES"):
                    execute_curl_command(batch, token)
    else:
        if user_confirmation(f"Are you ready to submit this for download? Estimated size: {total_file_size_gb:.2f} GB (Type YES to continue): ", "YES"):
            execute_curl_command(file_ids, token)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and parse JGI data for given taxon_oids.")
    parser.add_argument("--tsv", required=True, type=str, help="Path to the input TSV file with taxon_oids.")
    parser.add_argument("--output", required=True, type=str, help="Path to the output TSV file.")
    parser.add_argument("--metadata_output", required=True, type=str, help="Path to the output metadata TSV file.")
    parser.add_argument("--token", required=True, type=str, help="Authorization token for API access.")

    args = parser.parse_args()
    main(args.tsv, args.output, args.metadata_output, args.token)
