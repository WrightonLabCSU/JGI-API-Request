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

def parse_json_data(data, include_bins=True, include_assemblies=True, include_reads=True):
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
    externally_sequenced = 'no'
    latitude = 'N/A'
    longitude = 'N/A'

    organisms = data.get('organisms', [])
    for organism in organisms:
        top_hit = organism.get('top_hit')
        if top_hit:
            jamo_id = top_hit.get('_id', 'id-not-found')
            sequencing_project = top_hit.get('metadata', {}).get('sequencing_project', {})
            if sequencing_project.get('scientific_program_name', '').lower() == 'metagenome':
                externally_sequenced = 'yes'

        for file in organism.get('files', []):
            file_name = file.get('file_name', '')
            file_id = file.get('_id', '')
            file_size = file.get('file_size', 0)
            file_status = file.get('metadata', {}).get('data_utilization_status', 'Unknown')

            if include_assemblies and 'scaffolds.fasta' in file_name:
                assembly_fasta_filename = file_name
                assembly_fasta_file_id = file_id
                assembly_fasta_file_size = file_size if file_size > 0 else assembly_fasta_file_size
                assembly_fasta_file_status = file_status

            if include_reads and file_name.endswith('fastq.gz') and 'Raw Data' in file.get('metadata', {}).get('portal', {}).get('display_location', []):
                raw_reads_filename = file_name
                raw_reads_file_id = file_id
                raw_reads_file_size = file_size if file_size > 0 else raw_reads_file_size
                raw_reads_file_status = file_status

            if include_bins and file.get('metadata', {}).get('content_type') == 'Binning Data' and file_name.endswith('.tar.gz'):
                bins_fasta_filenames.append(file_name)
                bins_fasta_file_ids.append(file_id)
                bins_fasta_file_sizes.append(file_size if file_size > 0 else 0)
                bins_fasta_file_statuses.append(file_status)

            # Extract latitude and longitude from file metadata if available
            file_metadata = file.get('metadata', {})
            sow_segment = file_metadata.get('sow_segment', {})
            gold_data = file_metadata.get('gold_data', {})

            # Check for latitude and longitude in sow_segment
            if 'latitude_of_sample_collection' in sow_segment and 'longitude_of_sample_collection' in sow_segment:
                latitude = sow_segment.get('latitude_of_sample_collection', latitude)
                longitude = sow_segment.get('longitude_of_sample_collection', longitude)

            # Check for latitude and longitude in gold_data
            if 'latitude' in gold_data and 'longitude' in gold_data:
                latitude = gold_data.get('latitude', latitude)
                longitude = gold_data.get('longitude', longitude)

    bin_count = len(bins_fasta_filenames) if include_bins else 0  # Actual count of bin files

    return (jamo_id, assembly_fasta_filename, assembly_fasta_file_id, assembly_fasta_file_size, assembly_fasta_file_status,
            raw_reads_filename, raw_reads_file_id, raw_reads_file_size, raw_reads_file_status,
            ";".join(bins_fasta_filenames), ";".join(bins_fasta_file_ids), 
            ";".join(map(str, bins_fasta_file_sizes)), ";".join(bins_fasta_file_statuses), bin_count,
            externally_sequenced, latitude, longitude)

def extract_additional_metadata(organisms):
    # Initialize with default values
    additional_metadata = {
        'agg_id': 'N/A',
        'kingdom': 'N/A',
        'label': 'N/A',
        'country': 'N/A',
        'institution': 'N/A',
        'its_sp_id': 'N/A',
        'its_ap_id': 'N/A',
        'PI_name': 'N/A',
        'Email': 'N/A',
        'analysis_project_name': 'N/A'
    }

    if organisms:
        organism = organisms[0]  # Assume we want to extract from the first organism
        pi_info = organism.get('top_hit', {}).get('metadata', {}).get('proposal', {}).get('pi', {})
        analysis_project_info = organism.get('top_hit', {}).get('metadata', {}).get('analysis_project', {})

        # Get sequencing project id safely
        sequencing_projects = analysis_project_info.get('sequencing_projects', [])
        its_sp_id = sequencing_projects[0].get('sequencing_project_id', 'N/A') if sequencing_projects else 'N/A'

        additional_metadata.update({
            'agg_id': organism.get('agg_id', 'N/A'),
            'kingdom': organism.get('kingdom', 'N/A'),
            'label': organism.get('label', 'N/A'),
            'country': pi_info.get('country', 'N/A'),
            'institution': pi_info.get('institution', 'N/A'),
            'its_sp_id': its_sp_id,
            'its_ap_id': organism.get('top_hit', {}).get('metadata', {}).get('analysis_project_id', 'N/A'),
            'PI_name': f"{pi_info.get('first_name', '')} {pi_info.get('middle_name', '').strip() + ' ' if pi_info.get('middle_name') else ''}{pi_info.get('last_name', '')}".strip(),
            'Email': pi_info.get('email_address', 'N/A'),
            'analysis_project_name': analysis_project_info.get('analysis_project_name', 'N/A')
        })

    return additional_metadata

def execute_curl_command(file_ids, token):
    file_ids_str = ','.join(f'"{fid}"' for fid in file_ids)
    curl_cmd = [
        'curl', '-X', 'POST', "https://files.jgi.doe.gov/request_archived_files/", 
        '-H', 'accept: application/json', '-H', f"Authorization: {token}", 
        '-H', 'Content-Type: application/json', 
        '-d', f'{{ "ids": [{file_ids_str}], "send_mail": true, "api_version": "2"}}'
    ]
    subprocess.run(curl_cmd)

def user_confirmation(prompt, required_response):
    response = input(prompt)
    return response.strip().upper() == required_response

def calculate_total_file_size(output_path):
    total_file_size = 0
    with open(output_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            # Handle None for assembly fasta file sizes
            if row['assembly_fasta_file_size'] and row['assembly_fasta_file_size'].isdigit():
                total_file_size += int(row['assembly_fasta_file_size'])
            
            # Handle None for raw reads file sizes
            if row['raw_reads_file_size'] and row['raw_reads_file_size'].isdigit():
                total_file_size += int(row['raw_reads_file_size'])
            
            # Handle None for bins fasta file sizes
            bins_sizes = row['bins_fasta_file_size'].split(';') if row['bins_fasta_file_size'] else []
            total_file_size += sum(int(size) for size in bins_sizes if size.isdigit())
    
    return total_file_size / (1024 ** 3)  # Convert bytes to GB

def extract_file_ids(output_path):
    """Extract file IDs from the output file for curl commands."""
    file_ids = []
    file_sizes = []  # You also need the sizes to split the file IDs accurately by size
    with open(output_path, 'r') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            # Safely handle potentially None values
            if row['assembly_fasta_file_id']:
                file_ids.append(row['assembly_fasta_file_id'])
                file_sizes.append(int(row['assembly_fasta_file_size']) if row['assembly_fasta_file_size'].isdigit() else 0)
            if row['raw_reads_file_id']:
                file_ids.append(row['raw_reads_file_id'])
                file_sizes.append(int(row['raw_reads_file_size']) if row['raw_reads_file_size'].isdigit() else 0)
            bins_ids = row['bins_fasta_file_id'].split(';') if row['bins_fasta_file_id'] else []
            bins_sizes = row['bins_fasta_file_size'].split(';') if row['bins_fasta_file_size'] else []
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

def main(tsv_path, output_path, metadata_output_path, token, include_bins, include_assemblies, include_reads):
    with open(tsv_path, 'r') as tsv_file, open(output_path, 'w', newline='') as out_file, open(metadata_output_path, 'w', newline='') as meta_out_file:
        reader = csv.reader(tsv_file, delimiter='\t')
        writer = csv.writer(out_file, delimiter='\t')
        meta_writer = csv.writer(meta_out_file, delimiter='\t')

        # Regular output headers
        output_headers = [
            'taxon_oid', 'jamo_id', 'assembly_fasta_filename', 'assembly_fasta_file_id', 
            'assembly_fasta_file_size', 'assembly_fasta_file_status', 'raw_reads_filename', 
            'raw_reads_file_id', 'raw_reads_file_size', 'raw_reads_file_status', 'bins_fasta_filename', 
            'bins_fasta_file_id', 'bins_fasta_file_size', 'bins_fasta_file_status'
        ]
        
        # Metadata output headers
        meta_headers = [
            'taxon_oid', 'JAMO id', 'agg_id', 'kingdom', 'label', 'country', 'institution', 
            'its_sp_id', 'its_ap_id', 'PI_name', 'Email', 'analysis_project_name', 'Bin_count', 
            'Latitude', 'Longitude', 'External'
        ]
        
        writer.writerow(output_headers)
        meta_writer.writerow(meta_headers)

        next(reader, None)  # Skip header row


        for row in reader:
            taxon_oid = row[0]
            data = fetch_json_data(taxon_oid)
            if data and 'organisms' in data and data['organisms']:
                parsed_data = parse_json_data(data, include_bins=include_bins, include_assemblies=include_assemblies, include_reads=include_reads)
                additional_metadata = extract_additional_metadata(data['organisms'])
                # Write regular output without Bin_count, Latitude, Longitude, and External
                writer.writerow([taxon_oid] + list(parsed_data[:-4]))  # Exclude last 4 for regular output
                # Write metadata output including Bin_count, Latitude, Longitude, and External
                meta_writer.writerow([taxon_oid] + [parsed_data[0]] + list(additional_metadata.values()) + [parsed_data[-4], parsed_data[-2], parsed_data[-1], parsed_data[-3]])
            else:
                print(f"No organism data available for taxon_oid {taxon_oid}")
                default_data = [taxon_oid] + ['N/A']*13  # Correct number of 'N/A's for the regular output
                writer.writerow(default_data)
                default_meta_data = [taxon_oid] + ['N/A']*12 + ['N/A', 'N/A', 'N/A', 'N/A']  # Correct number of 'N/A's for the metadata output with the 4 new fields
                meta_writer.writerow(default_meta_data)

    # After processing all rows
    total_file_size_gb = calculate_total_file_size(output_path)
    file_ids, file_sizes = extract_file_ids(output_path)  # Fetch file IDs and sizes

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
    parser.add_argument("--no_bins", action='store_true', help="Exclude bin files from processing.")
    parser.add_argument("--no_assemblies", action='store_true', help="Exclude assembly files from processing.")
    parser.add_argument("--no_reads", action='store_true', help="Exclude raw reads files from processing.")

    args = parser.parse_args()
    main(args.tsv, args.output, args.metadata_output, args.token, not args.no_bins, not args.no_assemblies, not args.no_reads)
