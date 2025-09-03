param(
[string]$Folder = "examples/company_demo",
[string]$Out = "demo_out",
[switch]$UseAgents = $false, # set -UseAgents for the 2R pipeline
[switch]$Render = $true # render PNG if Graphviz is available
)


# ensure output dir exists
New-Item -ItemType Directory -Force $Out | Out-Null


# thesis-safe scan: static + heuristic mapper
if ($UseAgents) {
scriptgraph agents $Folder --roles 2R --out $Out
} else {
scriptgraph all $Folder --out $Out
}


# If Graphviz is installed, render PNG
if ($Render -and (Get-Command dot -ErrorAction SilentlyContinue)) {
dot -Tpng "$Out/graph.dot" -o "$Out/graph.png"
}


# zip artifacts for easy sharing
if (Test-Path "$Out.zip") { Remove-Item "$Out.zip" -Force }
Compress-Archive -Path "$Out/*" -DestinationPath "$Out.zip" -Force


Write-Host "\nDemo artifacts created:"
Write-Host " $Out/graph.yaml"
Write-Host " $Out/graph.dot"
if (Test-Path "$Out/graph.png") { Write-Host " $Out/graph.png" }
Write-Host " $Out.zip (share this)"