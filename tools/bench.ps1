param(
  [string]$DataRoot = "data/bundles",
  [string[]]$Roles = @("2R","4R"),
  [string]$Config = "config.example.yaml"
)
$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force artifacts | Out-Null
Remove-Item artifacts\bench_results.jsonl -ErrorAction SilentlyContinue

$truthFiles = Get-ChildItem -Path $DataRoot -Recurse -Filter truth.yaml
foreach ($tf in $truthFiles) {
  $truth = $tf.FullName
  $bundleDir = $tf.DirectoryName
  $bundleName = Split-Path $bundleDir -Leaf
  foreach ($r in $Roles) {
    $out = "artifacts/$($bundleName)-$r"
    scriptgraph agents $bundleDir --roles $r --out $out --config $Config | Out-Null
    $pred = "$out/predicted_graph.yaml"
    $score = scriptgraph score --pred $pred --truth $truth --pred-prefix $bundleDir | Out-String | ConvertFrom-Json
    $obj = @{ bundle=$bundleName; role=$r; score = $score }
    $line = $obj | ConvertTo-Json -Depth 6
    Add-Content -Path "artifacts/bench_results.jsonl" -Value $line
  }
}
Write-Host "Wrote artifacts/bench_results.jsonl"