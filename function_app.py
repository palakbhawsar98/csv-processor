import azure.functions as func
import logging
import csv
import io
import os
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="event")
def CsvCleanProcessor(event: func.EventGridEvent):

    # Get blob name and url from event
    data = event.get_json()
    blob_url = data.get("url", "")
    name = blob_url.split("/")[-1]

    # Step 1 — skip non CSV files
    if not name.lower().endswith(".csv"):
        logging.warning(f"Skipping {name} — not a CSV file")
        return

    # Step 2 — skip files not in raw-uploads
    if "raw-uploads" not in blob_url:
        logging.warning(f"Skipping {name} — not in raw-uploads container")
        return

    logging.info(f"Started processing: {name}")

    # Step 3 — fetch connection string from Key Vault
    kv_url = os.environ["KEY_VAULT_URL"]
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=kv_url, credential=credential)
    conn_str = secret_client.get_secret("stcsvprocessor01-connection-string").value

    # Step 4 — read the file from storages
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    raw_blob = blob_service.get_blob_client(
        container="raw-uploads",
        blob=name
    )
    content = raw_blob.download_blob().readall().decode("utf-8-sig")

    # Step 5 — parse the CSV
    reader = csv.DictReader(io.StringIO(content))

    if not reader.fieldnames:
        logging.error(f"File {name} is empty or has no headers. Stopping.")
        return

    # Step 6 — loop through rows and clean
    cleaned_rows = []
    skipped_rows = []
    total_count = 0

    for i, row in enumerate(reader, start=1):
        total_count += 1

        # Clean whitespace from every cell
        row = {k.strip(): v.strip() for k, v in row.items() if k}

        # Skip empty rows
        if not any(row.values()):
            skipped_rows.append({"row": i, "reason": "empty row"})
            continue

        # Skip rows with negative numbers
        has_invalid = False
        for col, val in row.items():
            try:
                if float(val) < 0:
                    skipped_rows.append({"row": i, "reason": f"negative value in '{col}': {val}"})
                    has_invalid = True
                    break
            except ValueError:
                pass

        if not has_invalid:
            cleaned_rows.append(row)

    # Step 7 — log summary
    logging.info(
        f"File: {name} | "
        f"Total: {total_count} | "
        f"Cleaned: {len(cleaned_rows)} | "
        f"Skipped: {len(skipped_rows)}"
    )

    for skip in skipped_rows:
        logging.warning(f"Skipped row {skip['row']}: {skip['reason']}")

    if not cleaned_rows:
        logging.error(f"No valid rows in {name}. No output file written.")
        return

    # Step 8 — write cleaned CSV to processed-uploads
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=cleaned_rows[0].keys())
    writer.writeheader()
    writer.writerows(cleaned_rows)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_name = name.replace(".csv", "").replace(".CSV", "")
    output_filename = f"{base_name}_{timestamp}_cleaned.csv"

    output_blob = blob_service.get_blob_client(
        container="processed-uploads",
        blob=output_filename
    )
    output_blob.upload_blob(
        output.getvalue().encode("utf-8"),
        overwrite=True
    )

    logging.info(f"Successfully written: processed-uploads/{output_filename}")
