# Sets Athena/AWS env vars for the current PowerShell session.
# Must be dot-sourced, not just run, or the variables won't persist
# in your terminal: . .\set-env.ps1

$env:AWS_PROFILE = "superstore-demo"
$env:AWS_REGION = "us-east-1"
$env:AWS_DEFAULT_REGION = "us-east-1"
$env:ATHENA_DATABASE = "sample"
$env:ATHENA_OUTPUT = "s3://superstore-demo-937749309165-us-east-1/athena-results/"

Write-Host "Environment configured for superstore-demo (AWS_PROFILE=$env:AWS_PROFILE, ATHENA_DATABASE=$env:ATHENA_DATABASE)"
