param(
  [string]$DataRoot = "data/bundles",
  # We benchmark three systems: static (scan only), 2R and 4R
  [string[]]$Systems = @("static","2R","4R"),
  [string]$Config = "config.example.yaml"
)
$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force artifacts | Out-Null
$outFile = "artifacts/bench_results.jsonl"
Remove-Item $outFile -ErrorAction SilentlyContinue

$truthFiles = Get-ChildItem -Path $DataRoot -Recurse -Filter truth.yaml
foreach ($tf in $truthFiles) {
  $truth      = $tf.FullName
  $bundleDir  = $tf.DirectoryName
  $bundleName = Split-Path $bundleDir -Leaf
  $kind       = Split-Path -Leaf (Split-Path -Parent $bundleDir)  # "easy" or "hard"
  $bundleLabel = "$kind-$bundleName"

foreach ($sys in $Systems) {
    $out = "artifacts/$($bundleLabel)-$sys"
    if ($sys -eq "static") {
      # static baseline: scanner only (no agent mapping)
      scriptgraph scan $bundleDir --out $out --config $Config | Out-Null
    } else {
      scriptgraph agents $bundleDir --roles $sys --out $out --config $Config | Out-Null
    }
    $pred = "$out/predicted_graph.yaml"
    # score (Windows bundles are case-insensitive)
    $metaPath = Join-Path $bundleDir "meta.json"
    $caseFlag = ""
    if (Test-Path $metaPath) {
      try {
        $meta = Get-Content $metaPath -Raw | ConvertFrom-Json
        if ($meta.platform -eq "windows") { $caseFlag = "--case-insensitive" }
      } catch {}
    }
    $score = scriptgraph score --pred $pred --truth $truth $caseFlag | Out-String | ConvertFrom-Json
    $obj = @{ bundle=$bundleLabel; role=$sys; score = $score }
    $line = $obj | ConvertTo-Json -Depth 10 -Compress
    Add-Content -Path $outFile -Value $line
  }
}
Write-Host "Wrote $outFile"