param(
  [string]$DataRoot = "data/bundles",
  [string]$Config   = "config.example.yaml",
  [string[]]$Systems = @("static","2R","4R"),

  # Budgets to sweep; these override the agents via SG_MAX_FILES
  [int[]]$MaxFiles = @(20, 40, 60),

  [int]$EasyNoiseDirs = 0,
  [int]$EasyNoiseFiles = 0,
  [int]$HardNoiseDirs = 8,
  [int]$HardNoiseFiles = 15,
  # Optional: regenerate bundles before ablation
  [switch]$Regenerate,
  [string]$OutRoot = "artifacts",
  [int]$Easy = 100,
  [int]$Hard = 50,
  [int]$Seed = 13,
  [ValidateSet("linux","windows","mixed")] [string]$Platform = "mixed",
  [int]$MinHardFeatures = 3,
  [int]$NoiseDirs  = 0,
  [int]$NoiseFiles = 0
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force $OutRoot | Out-Null
$ablateDir = Join-Path $OutRoot "ablate"
New-Item -ItemType Directory -Force $ablateDir | Out-Null
$combined = Join-Path $ablateDir "bench_ablate.jsonl"
Remove-Item $combined -ErrorAction SilentlyContinue

if ($Regenerate) {
  Write-Host "Regenerating dataset..." -ForegroundColor Cyan
  & .\tools\make_bundles.ps1 `
    -Out $DataRoot -Easy $Easy -Hard $Hard -Seed $Seed -Platform $Platform `
    -MinHardFeatures $MinHardFeatures -NoiseDirs $NoiseDirs -NoiseFiles $NoiseFiles
}

foreach ($mf in $MaxFiles) {
  Write-Host ("--- Running bench with SG_MAX_FILES={0} ---" -f $mf) -ForegroundColor Yellow
  $env:SG_MAX_FILES = "$mf"

  # Run the existing bench; it writes artifacts/bench_results.jsonl
  & .\tools\bench.ps1 -DataRoot $DataRoot -Systems $Systems -Config $Config

  $benchFile = Join-Path $OutRoot "bench_results.jsonl"
  if (-not (Test-Path $benchFile)) {
    throw "bench_results.jsonl not found; bench.ps1 failed?"
  }

  # Merge in 'max_files' and latencies from per-run run_stats.json
  Get-Content $benchFile | ForEach-Object {
    if (-not $_) { return }
    try {
      $obj = $_ | ConvertFrom-Json
    } catch { return }

    $bundleLabel = [string]$obj.bundle  # e.g., "hard-003"
    $sys         = [string]$obj.role    # "static" | "2R" | "4R"
    $outDir      = Join-Path $OutRoot ("{0}-{1}" -f $bundleLabel, $sys)
    $statsPath   = Join-Path $outDir "run_stats.json"

    $obj.max_files = $mf
    if (Test-Path $statsPath) {
      try {
        $stats = Get-Content $statsPath -Raw | ConvertFrom-Json
        $obj.latency_ms = $stats.latency_ms
        $obj.total_ms   = $stats.latency_ms.total
      } catch {}
    }

    ($obj | ConvertTo-Json -Depth 12 -Compress) | Add-Content -Path $combined
  }
}

Write-Host "Combined JSONL written to $combined" -ForegroundColor Green

# Optional: create CSV + plot
python tools/ablate_plot.py --jsonl $combined --out-dir $ablateDir
Write-Host ("Wrote CSV/plots under {0}" -f $ablateDir) -ForegroundColor Green
