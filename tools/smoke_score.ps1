param(
  [string]$Folder = "data/bundles/examples/bundle1",
  [string]$Truth = "data/bundles/examples/bundle1/truth.yaml",
  [string]$Out = "out",
  [string]$Config = "config.example.yaml"
)
$ErrorActionPreference = "Stop"

Write-Host "== scan+map (no egress default) =="
scriptgraph all $Folder --out $Out --config $Config

if (-not (Test-Path "$Out/predicted_graph.yaml")) {
  throw "No predicted_graph.yaml in $Out"
}

Write-Host "== score =="
scriptgraph score --pred "$Out/predicted_graph.yaml" --truth $Truth --pred-prefix $Folder

Write-Host "`n== agents 2R (Reader+Mapper only) =="
scriptgraph agents $Folder --roles 2R --out $Out --config $Config
scriptgraph score --pred "$Out/predicted_graph.yaml" --truth $Truth --pred-prefix $Folder

Write-Host "`n== agents 4R (full pipeline) =="
scriptgraph agents $Folder --roles 4R --out $Out --config $Config
scriptgraph score --pred "$Out/predicted_graph.yaml" --truth $Truth --pred-prefix $Folder

Write-Host "`n== run stats =="
scriptgraph stats runs --config $Config