# Process function Secrets passed in
$VC_CONFIG_FILE = "/var/openfaas/secrets/vc-slack-config"
$VC_CONFIG = (Get-Content -Raw -Path $VC_CONFIG_FILE | ConvertFrom-Json)
if($env:function_debug -eq "true") {
    Write-host "DEBUG: `"$VC_CONFIG`""
}

# Process payload sent from vCenter Server Event
$json = $args | ConvertFrom-Json
if($env:function_debug -eq "true") {
    Write-Host "DEBUG: json=`"$($json | Format-List | Out-String)`""
}

$entity = $json.data.eventEntity.name

# import and configure Slack
Import-Module PSSlack | Out-Null

# Retrieve VM changes
$Message = $json.data.description

# Bold format for titles
# [string]$Message = $Message -replace "Modified","*Modified*" -replace "Added","*Added*" -replace "Deleted","*Deleted*"

# Send VM changes
Write-Host "VCD Task completed for $entity..."

New-SlackMessageAttachment -Color $([System.Drawing.Color]::red) `
                           -Title 'VCD Task completed' `
                           -Text "$Message" `
                           -Fallback 'ouch' |
    New-SlackMessage -Channel $($VC_CONFIG.SLACK_CHANNEL) `
                     -IconEmoji :fire: |
    Send-SlackMessage -Uri $($VC_CONFIG.SLACK_URL)

