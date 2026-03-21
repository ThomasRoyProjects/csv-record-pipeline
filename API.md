# Local API

This project includes a small local HTTP API behind the web UI in [webapp/server.py](webapp/server.py).

Base URL:

```text
http://127.0.0.1:8765
```

Use this API for local tooling, browser automation, and debugging the web app. It is not an authenticated remote service.

## Start the server

From the repo root:

```bash
./run_webapp.sh
```

Health check:

```bash
curl -I http://127.0.0.1:8765/
```

## Response shape

Most JSON endpoints return:

- `ok`: boolean for mutating endpoints
- `errors`: list of strings when validation or execution fails

Run-oriented endpoints may also return:

- `workflow`
- `summary_path`
- `summary`
- `job_id`
- `status`

## GET endpoints

### `GET /api/workflows`

Returns workflow metadata used by the picker UI.

Includes:

- workflow id
- label
- detailed description object
- required and recommended fields

### `GET /api/workflows/<workflow>`

Returns one workflow description payload.

Useful for:

- preset detail panes
- external tooling that wants workflow-specific input expectations

### `GET /api/stages`

Returns engine stage metadata.

Includes:

- stage name
- label
- summary
- operator goal
- required input roles
- effects
- watch-for notes

### `GET /api/normalization-profiles`

Returns available saved normalization profiles from [normalization_profiles](normalization_profiles).

### `GET /api/file-inventory`

Returns tracked local files from managed directories:

- `input/raw`
- `input/normalized`
- `output`

Each file payload includes:

- name
- absolute path
- relative path
- size
- modified timestamp
- download URL

### `GET /api/presets`

Returns saved web UI presets.

### `GET /api/presets/<name>`

Returns one saved preset payload.

### `GET /api/headers?path=<abs_path>`

Returns CSV headers for a file.

Response:

```json
{
  "headers": ["first_name", "last_name", "email"]
}
```

### `GET /api/suggest-mapping?path=<abs_path>`

Returns:

- `headers`
- `suggestions`
- `groups`

This is the main inspection endpoint behind the Match tab.

`groups` clusters headers into broad families such as:

- identity
- email
- phone
- address
- dates
- money

### `GET /api/run-summary?path=<abs_path_to_run_summary.json>`

Loads an existing run summary file.

### `GET /api/job-status?id=<job_id>`

Returns async background job status.

Possible statuses:

- `queued`
- `running`
- `completed`
- `failed`

Completed jobs return `result`.
Failed jobs return `errors`.

### `GET /api/preview-csv?path=<abs_path>&limit=<n>`

Returns a small preview payload:

- `path`
- `columns`
- `rows`

Used by the web UI for result previews.

### `GET /api/download-file?path=<abs_path>`

Streams a managed file back to the client.

## POST endpoints

### `POST /api/validate-job`

Builds runtime config from a payload and validates it.

Request body:

```json
{
  "template": "jobs/demo_match_job.yaml"
}
```

or a direct runtime-style payload built from the UI.

Response:

```json
{
  "runtime": { "...": "..." },
  "errors": []
}
```

### `POST /api/run-job`

Runs a job synchronously and returns when complete.

Useful for:

- lighter jobs
- debugging

For heavier jobs, prefer `POST /api/run-job-async`.

### `POST /api/run-job-async`

Starts a job in a local background thread and returns immediately.

Response:

```json
{
  "ok": true,
  "job_id": "abcd1234",
  "status": "queued",
  "workflow": "match_records_to_reference",
  "result": null,
  "errors": []
}
```

Then poll:

```text
GET /api/job-status?id=abcd1234
```

This is the recommended run path for:

- `match_records_to_reference`
- `custom_job`
- `process_full_records`

### `POST /api/save-preset`

Saves a web UI preset.

Request body:

```json
{
  "name": "my-preset",
  "preset": { "...": "..." }
}
```

### `POST /api/upload-file`

Writes uploaded file content into `input/raw`.

Request body:

```json
{
  "filename": "input.csv",
  "content": "a,b,c\n1,2,3\n"
}
```

### `POST /api/delete-file`

Deletes a managed file only if it is under:

- `input/raw`
- `input/normalized`
- `output`

Request body:

```json
{
  "path": "/absolute/path/to/file.csv"
}
```

### `POST /api/delete-all-outputs`

Deletes all files under [output](output).

### `POST /api/run-normalization`

Runs the Normalize-tab cleaning path.

Request body:

```json
{
  "input_path": "/absolute/path/to/input.csv",
  "profile_name": "",
  "output_name": "normalized_output_name",
  "strict_text_cleanup": false,
  "columns": {
    "person_id": "ConstituantID",
    "primary_address1": "MailingAddress"
  }
}
```

Behavior:

- output is always written under `input/normalized`
- output file names auto-increment instead of overwriting existing files
- built-in address normalization runs after canonical mapping

### `POST /api/save-normalization-profile`

Saves a normalization profile into [normalization_profiles](normalization_profiles).

## Example async flow

Start a match job:

```bash
curl -s -X POST http://127.0.0.1:8765/api/run-job-async \
  -H 'Content-Type: application/json' \
  --data '{"template":"jobs/demo_match_job.yaml"}'
```

Poll status:

```bash
curl -s "http://127.0.0.1:8765/api/job-status?id=<job_id>"
```

## Current limits

- background jobs are stored in memory only
- there is no persistent run history API
- there is no authentication layer
- file paths are local absolute paths
- the API is intended for local use, not exposure to a network
