param(
  [string]$Out = "data/bundles",
  [int]$Easy = 250,
  [int]$Hard = 50,
  [int]$Seed = 13,
  [ValidateSet("linux","windows","mixed")] [string]$Platform = "mixed",
  [int]$MinHardFeatures = 3,
  [switch]$Verify
)
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
python tools/generate_bundles.py --out $Out --kind easy --count $Easy --seed $Seed --platform $Platform
python tools/generate_bundles.py --out $Out --kind hard --count $Hard --seed $Seed --platform $Platform --min-hard-features $MinHardFeatures

if ($Verify) {
  Write-Host "Verifying hard bundle difficulty..." -ForegroundColor Cyan
  $hardDirs = Get-ChildItem -Directory -Path (Join-Path $Out "hard") -ErrorAction SilentlyContinue
  $ok = $true
  if ($hardDirs) {
    foreach ($dir in $hardDirs) {
      $m = Join-Path $dir.FullName "meta.json"
      if (Test-Path $m) {
        $meta = Get-Content $m -Raw | ConvertFrom-Json
        $features = @($meta.features)
        $dynamic = @("var-indirection","delayed-expansion","dot-sourcing","interpreter-hop-bash","interpreter-hop-python","interpreter-hop-perl","for-loop","cross-language")
        $hasDynamic = ($features | Where-Object { $dynamic -contains $_ }).Count -ge 1
        if (-not $hasDynamic -or $features.Count -lt $MinHardFeatures) {
          Write-Host ("[FAIL] {0} features: {1}" -f $dir.Name, ($features -join ",")) -ForegroundColor Red
          $ok = $false
        } else {
          Write-Host ("[OK]   {0} features: {1}" -f $dir.Name, ($features -join ",")) -ForegroundColor Green
        }
      }
    }
  }
  if (-not $ok) { throw "Some hard bundles do not meet the difficulty threshold." }
}
Write-Host "Done. Bundles in $Out\easy and $Out\hard"
