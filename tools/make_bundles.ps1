param(
  [string]$Out = "data/bundles",
  [int]$Easy = 250,
  [int]$Hard = 50,
  [int]$Seed = 13,
  [ValidateSet("linux","windows","mixed")] [string]$Platform = "mixed",
  [int]$MinHardFeatures = 3,

  # NEW: noise controls (decoys) – separate for easy/hard
  [int]$EasyNoiseDirs = 0,
  [int]$EasyNoiseFiles = 0,
  [int]$HardNoiseDirs = 8,
  [int]$HardNoiseFiles = 15,

  [switch]$Verify
)

$ErrorActionPreference = "Stop"

py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .

# easy (force no noise regardless of parameters)
python tools/generate_bundles.py `
  --out $Out `
  --kind easy `
  --count $Easy `
  --seed $Seed `
  --platform $Platform `
  --noise-scope none `
  --noise-dirs 0 `
  --noise-files 0

# hard (noise only here)
python tools/generate_bundles.py `
  --out $Out `
  --kind hard `
  --count $Hard `
  --seed $Seed `
  --platform $Platform `
  --min-hard-features $MinHardFeatures `
  --noise-scope hard `
  --noise-dirs $HardNoiseDirs `
  --noise-files $HardNoiseFiles

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
        $dynamic = @(
          "var-indirection","delayed-expansion","dot-sourcing",
          "interpreter-hop-bash","interpreter-hop-python","interpreter-hop-perl",
          "for-loop","cross-language","multi-hop"
        )
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
