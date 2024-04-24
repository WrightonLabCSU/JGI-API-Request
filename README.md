# JGI Data Fetch and Process Script

This Python script is designed to fetch and process genomic data from the Joint Genome Institute (JGI) API, allowing users to request file downloads programmatically based on given taxon IDs.

**This is hard-coded to download assembly files (fasta), bin files (fasta) and raw data (fastq) files.**

**If you want other files, reach out to Reed.**


*Note:* Even if you answer "No" to the final prompt to download the files, you will still get the generated metadata table and output table.


## Inputs and Outputs

### Inputs
- **TSV File (`--tsv`)**: A tab-separated values file containing taxon OIDs (one per line) which are used to fetch data from the JGI API. **The taxon oid values must be in the first column and it needs a column name (no name requirement).**
- **Output TSV File (`--output`)**: The file name the script will write the parsed data, including file names and IDs necessary for download requests.
- **Metadata Output TSV File (`--metadata_output`)**: TThe file name the script will write additional metadata associated with each taxon OID.
- **Session Token (`--token`)**: An authorization token required to interact with the JGI API and download files.

### Outputs
- **Output TSV File**: Contains file information like names, IDs, and sizes necessary for download requests.
- **Metadata Output TSV File**: Contains additional metadata such as the kingdom, label, country, and institutional data associated with each taxon OID.

## Example Usage

```bash
python request-JGI-API.py --tsv input_tsv_path.tsv --output output_tsv_path.tsv --metadata_output metadata_output_tsv_path.tsv --token "Bearer your_api_session_token_here"
```

Replace `input_tsv_path.tsv`, `output_tsv_path.tsv`, `metadata_output_tsv_path.tsv`, and `your_api_session_token_here` with your actual file paths and API token.

## Limitations

- **File Size**: The script is designed to handle requests where the total data size does not exceed 10 terabytes. If the combined size of the files requested exceeds this threshold, the script will:
  - Warn the user about the large data size.
  - Offer to split the download into multiple smaller requests.
  - If agreed, it will manage the division such that each subset of files is less than 10 TB, making separate API calls for each batch.

## Example Use Case

### Input File
- **File Name**: `taxon_oids.csv`
  - This file contains taxon OIDs to fetch genomic data from the JGI API.

### Desired Output Files
- **Metadata Output File**: `test-metadata-output.tsv`
- **Parsed Data Output File**: `test-output.tsv`

### Command to Run the Script
```bash
python request-JGI-API.py --tsv taxon_oids.csv --output test-output.tsv --metadata_output test-metadata-output.tsv --token "Bearer your_api_session_token_here"
```

Replace `"Bearer your_api_session_token_here"` with your actual JGI API session token.

### Simulated Command-Line Interaction
Assuming the total file size from `taxon_oids.csv` is 15 TB, which is over the 10 TB limit:

```plaintext
Warning: The requested data set is very large (15000.00 GB).
Do you want to split the download into multiple smaller requests? (yes/no): yes
Proceed with downloading a batch of 20 files? (yes/no): yes
Running Curl command...

```

This interaction shows that the script will prompt the user due to the large data size, offer to split the download into smaller parts, and proceed with multiple smaller download requests once confirmed by the user.


## What Happens Next

Once the user replies "YES" to the prompts, a CURL command to request the files to be downloaded is submitted.

On the command line you will recieve an acknoledgement of your request which will list the number of files you wish to restore, a `restore_id` and a `request_status_url`:

`{"updated_count":0,"restored_count":1,"request_id":473828,"request_status_url":"https://files.jgi.doe.gov/request_archived_files/requests/473828"}`

Then you can navigate to this URL to see the status of your request.

You will then recieve and e-mail (connected to your JGI account which you used to get the token) once they are ready to be downloaded (time depends on the size of your request).


## How to Get the Token

To restore files or download files via the API, you will need to provide your session token. You can get this by clicking on **Copy My Session Token** in the avatar dropdown menu of the JGI browser application (after you log in).

For further details on how to interact with the JGI API, refer to the [JGI API Documentation](https://sites.google.com/lbl.gov/data-portal-help/home/tips_tutorials/api-tutorial?authuser=0#h.3dorflyfeai2).
