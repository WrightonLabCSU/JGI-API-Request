# JGI Data Fetch and Process Script

This Python script is designed to fetch and process genomic data from the Joint Genome Institute (JGI) API, allowing users to request file downloads programmatically based on given taxon IDs.

## Inputs and Outputs

### Inputs
- **TSV File (`--tsv`)**: A tab-separated values file containing taxon OIDs (one per line) which are used to fetch data from the JGI API.
- **Output TSV File (`--output`)**: The path where the script will write the parsed data, including file names and IDs necessary for download requests.
- **Metadata Output TSV File (`--metadata_output`)**: The path where the script will write additional metadata associated with each taxon OID.
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

## How to Get the Token

To restore files or download files via the API, you will need to provide your session token. You can get this by clicking on **Copy My Session Token** in the avatar dropdown menu of the JGI browser application (after you log in).

For further details on how to interact with the JGI API, refer to the [JGI API Documentation](https://sites.google.com/lbl.gov/data-portal-help/home/tips_tutorials/api-tutorial?authuser=0#h.3dorflyfeai2).
