. ./Env.ps1
$D1 = Join-Path $BASE $REL
$Full = Join-Path $D1 $NAME
& $Full
# ps-run // sg-salt:122890
