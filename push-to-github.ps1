# push-to-github.ps1
# Upload all files and folders in this directory to GitHub.
#
# Usage (from project folder):
#   .\push-to-github.ps1
#   .\push-to-github.ps1 "Your commit message"
#   .\push-to-github.ps1 "Your commit message" main

param(
    [string]$CommitMessage = "Update project",
    [string]$Branch = "main",
    [string]$RemoteName = "origin",
    [string]$RemoteUrl = "https://github.com/philsysppscavite2024-glitch/PhilSys2026Output.git"
)

$ErrorActionPreference = "Stop"

$ProjectRoot = $PSScriptRoot
if (-not $ProjectRoot) {
    $ProjectRoot = Get-Location
}
Set-Location $ProjectRoot
Write-Host "Project root: $ProjectRoot"

try {
    $null = git --version 2>$null
} catch {
    Write-Error "Git is not installed or not in PATH. Install from https://git-scm.com/downloads"
    exit 1
}

$gitignorePath = Join-Path $ProjectRoot ".gitignore"
if (-not (Test-Path $gitignorePath)) {
    @"
# Python
.venv/
venv/
__pycache__/
*.py[cod]
*.pyo
.Python
*.so

# IDE / OS
.vs/
.idea/
*.swp
Thumbs.db
Desktop.ini

# Flask / local data (uncomment to keep DB and uploads off GitHub)
# instance/
"@ | Set-Content -Path $gitignorePath -Encoding UTF8
    Write-Host "Created default .gitignore at $gitignorePath"
}

if (-not (Test-Path (Join-Path $ProjectRoot ".git"))) {
    Write-Host "Initializing git repository..."
    git init
}

$remotes = @(git remote 2>$null)
if ($remotes -contains $RemoteName) {
    Write-Host "Setting remote $RemoteName URL..."
    git remote set-url $RemoteName $RemoteUrl
} else {
    Write-Host "Adding remote $RemoteName..."
    git remote add $RemoteName $RemoteUrl
}

Write-Host "Staging all files..."
git add -A

$status = @(git status --porcelain 2>$null)
if ($status.Count -eq 0) {
    Write-Host "Nothing to commit (working tree clean)."
} else {
    Write-Host "Committing: $CommitMessage"
    git commit -m $CommitMessage
}

# First commit often lands on "master"; rename before push so refspec matches.
git rev-parse HEAD 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Ensuring branch name is $Branch ..."
    git branch -M $Branch
}

Write-Host "Pushing to $RemoteName $Branch ..."
git push -u $RemoteName $Branch

Write-Host "Done."
