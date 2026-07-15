# Running GenAI InsightPilot Explorer on Windows

These steps assume the AWS side (S3 bucket, Glue database `sample`, Athena table
`sample.super_store_data`, and the `SuperstoreDemoRole` / `superstore-demo-user`
IAM setup) already exists, and that you're copying the **same** IAM user access
key pair (`superstore-demo-user`) onto this machine. If you don't have that key
pair yet, generate it from IAM \> Users \> superstore-demo-user \> Security
credentials \> Create access key on the original setup first.

## 1. Prerequisites
Install, if not already present:
- [Python 3.11+](https://www.python.org/downloads/windows/) — during install, check "Add python.exe to PATH". If you don't have admin rights, use the "Install for me only" option in the installer (no admin needed), or grab Python from the Microsoft Store instead.
- [Git for Windows](https://git-scm.com/download/win) — also installable without admin via "Install for me only".

The **AWS CLI is not required**. The app talks to AWS through `boto3`
(already listed in `requirements.txt`), which reads the same
`%USERPROFILE%\.aws\credentials` / `config` files the CLI would use, but
doesn't need the CLI itself installed — one less admin-gated install.

Verify in PowerShell:
```powershell
python --version
git --version
```

## 2. Get the code
```powershell
git clone https://github.com/Amritabis10/genai-insightpilot-explorer.git
cd genai-insightpilot-explorer
```

## 3. Create a virtual environment
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```
If PowerShell blocks the activation script with an execution-policy error, run
this once (as your normal user, not admin) and retry:
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

## 4. Install dependencies
```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 5. Configure AWS credentials + role profile
Create `%USERPROFILE%\.aws\credentials`:
```ini
[superstore-demo-user]
aws_access_key_id = <paste the Access Key ID>
aws_secret_access_key = <paste the Secret Access Key>
```
Create `%USERPROFILE%\.aws\config`:
```ini
[profile superstore-demo]
role_arn = arn:aws:iam::937749309165:role/SuperstoreDemoRole
source_profile = superstore-demo-user
region = us-east-1
```
These two files are equivalent to `~/.aws/credentials` and `~/.aws/config` on
macOS/Linux — same format, just a different folder path. **Never commit these
files or paste the secret key into the repo.**

## 6. Set environment variables for the session
```powershell
$env:AWS_PROFILE = "superstore-demo"
$env:AWS_REGION = "us-east-1"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:ATHENA_DATABASE = "sample"
$env:ATHENA_OUTPUT = "s3://superstore-demo-937749309165-us-east-1/athena-results/"
```
These only last for the current PowerShell session. To persist them across
sessions, use `setx` instead (requires a new terminal window to take effect):
```powershell
setx AWS_PROFILE "superstore-demo"
setx AWS_REGION "us-east-1"
setx AWS_DEFAULT_REGION "us-east-1"
setx ATHENA_DATABASE "sample"
setx ATHENA_OUTPUT "s3://superstore-demo-937749309165-us-east-1/athena-results/"
```

## 7. Verify the role assumption works
No AWS CLI, so verify with a one-line boto3 check instead (run this with the
venv still active, from step 3):
```powershell
python -c "import boto3; print(boto3.client('sts').get_caller_identity())"
```
The `Arn` in the output should show
`assumed-role/SuperstoreDemoRole/...`, not the IAM user directly. If you get
`InvalidClientTokenId` or `AccessDenied`, double check the access key pair was
copied correctly and that both `.aws` files above are saved without a `.txt`
extension (Notepad likes to append one — use "Save As" \> "All Files" and
type the filename with quotes, e.g. `"credentials"`, to prevent that).

## 8. Run the app
```powershell
streamlit run ui.py
```
Always launch via `ui.py` at the repo root, not `src/app.py` directly — the
latter breaks the package-relative imports inside `src/`.

Streamlit opens `http://localhost:8501` in your default browser. Ask a
question like "fetch total orders across years" to confirm the full path
(role assumption → Athena query → Glue schema → S3 read → results in the UI)
works end to end.

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `ImportError: attempted relative import with no known parent package` | Ran `streamlit run src/app.py` | Run `streamlit run ui.py` instead |
| `AccessDeniedException ... bedrock:InvokeModel*` | Role's Bedrock permissions don't cover the model/inference-profile in use | Confirm the `BedrockInvoke` statement on `SuperstoreDemoRole` includes the profile ARN from the error message |
| `Unable to verify/create output bucket ...` | Role missing `s3:GetBucketLocation` on the Athena output bucket | Add that permission to `SuperstoreDemoRole` |
| `COLUMN_NOT_FOUND: ... cannot be resolved` | Sidebar schema text doesn't match actual Glue table column names | Already fixed in `src/constants.py` on this branch; make sure you pulled the latest commit |
