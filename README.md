# Accomplishment Report Dashboard

Pure Python dashboard for managing employee accomplishment data and exporting employee reports with logo and signatories.

## Features

- Employee master list
- Employee output encoding and editing
- City / Municipality record per month
- Signatory management with add, edit, and delete
- Google Sheet CSV import
- Google Apps Script JSON import
- Monthly employee PDF export
- Bulk export of all employee reports into a ZIP file

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Open `http://127.0.0.1:5000`.

## Import Notes

Use a CSV export URL like:

```text
https://docs.google.com/spreadsheets/d/<sheet-id>/export?format=csv&gid=<gid>
```

Your current Google Sheet export returned `401 Unauthorized`, so it likely needs to be publicly shared or accessed through credentials.

Your current Apps Script URL returned `Hindi mahanap ang script function: doGet`, so it does not currently expose a GET endpoint. The dashboard supports POST too, once you provide the payload/handler.

## Expected Import Fields

- Employee: `employee`, `employee_name`, `full_name`, `name`, `personnel`
- Code: `employee_code`, `code`, `employee_id`
- Date: `date`, `work_date`, `transaction_date`
- Category: `category`, `report_type`, `module`
- Activity: `activity_type`, `activity`, `service`
- Quantity: `quantity`, `qty`, `count`, `total`, `output`
- Remarks: `remarks`, `remark`, `notes`
- Location: `province`, `city_municipality`, `city`, `municipality`

## Next Step

If you send the Apps Script source and the exact logo file, the next pass can align the import mapping and PDF layout even closer to your real accomplishment report format.
