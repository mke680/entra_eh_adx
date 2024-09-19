function Send-BatchToEventHub { 

    <#
    .SYNOPSIS
        Uploads Json payload to blob and then ingests to ADX.
    .PARAMETER Payload
        Json payload with contents to be ingested into table
    .PARAMETER Resource
        Name of resources to send to event hub
    .PARAMETER Cluster
        Name of ADX Cluster
    .PARAMETER Database
        Name of ADX Database
    #>

    [CmdletBinding()]
    param (
        [Parameter(Mandatory,ValueFromPipeline)]
        [ValidateNotNullOrEmpty()]
        [System.Object]$Payload,
        [Parameter(Mandatory)]
        [string]$Resource,
        [Parameter(Mandatory)]
        [string]$Cluster,
        [Parameter(Mandatory)]
        [string]$Database,
        [Parameter(Mandatory)]
        [string]$Category
        [Parameter(Mandatory)]
        [string]$Key
    )

    # ADX Details 
    $clusterHost = "$($Cluster).australiaeast.kusto.windows.net"
    $clusterUri = "https://$($clusterHost)"
    $queryUri = "$($clusterUri)/v1/rest/mgmt"
    
    # ADX Table Details
    $table_suffix = $Category.Substring(0,2)
    $table = "$($Resource)_$($table_suffix)"
    #$TextInfo = (Get-Culture).TextInfo

    ## Upload to Event Hub
    function New-SasToken {
        param (
            [string]$resourceUri,
            [string]$keyName,
            [string]$key
        )
    
        $expiry = [int](Get-Date -UFormat %s) + 20000 
        $stringToSign = [System.Web.HttpUtility]::UrlEncode($resourceUri) + "`n" + $expiry
        $keyBytes = [System.Text.Encoding]::UTF8.GetBytes($key)
        $signatureBytes = [System.Text.Encoding]::UTF8.GetBytes($stringToSign)
    
        $hmac = New-Object System.Security.Cryptography.HMACSHA256
        $hmac.Key = $keyBytes
        $signature = $hmac.ComputeHash($signatureBytes)
        $signatureEncoded = [System.Web.HttpUtility]::UrlEncode([System.Convert]::ToBase64String($signature))
        $resourceUri = [System.Web.HttpUtility]::UrlEncode($resourceUri)
    
        return "SharedAccessSignature sr=$resourceUri&sig=$signatureEncoded&se=$expiry&skn=$keyName"
    }
    
    
    # Replace with your Event Hub details
    $eventHubNamespace = "ehns-poc-aue-001"
    $eventHubName = "eh-poc-aue-json-$($table.ToLower())"
    $keyName = "RootManageSharedAccessKey"
    
    $resourceUri = "$($eventHubNamespace).servicebus.windows.net/$($eventHubName)"
    $sasToken = New-SasToken -resourceUri $resourceUri -keyName $keyName -key $key
    
    # Define the Event Hub endpoint
    $url = "https://$($resourceUri)/messages"
    
    # Set the headers
    $headers = @{
        "Authorization" = $sasToken
        "Content-Type" = "application/json"
    }

    $paginatedJson = [System.Collections.Concurrent.ConcurrentBag[psobject]]::new()
    $currentSize = 0
    $maxSize = 0.9MB

    foreach($item in $Payload){
        $jsonItem = $item | ConvertTo-Json -Depth 100
        $jsonSize = [System.Text.Encoding]::UTF8.GetByteCount($jsonItem)
        if($currentSize + $jsonSize -gt $maxSize){
            # Send the POST request
            try{
                Write-Output "Sending Batch to Event Hub with $($currentSize) bytes"
                Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $($paginatedJson | ConvertTo-Json -Depth 100)
                # Output the response
                Write-Verbose "Event sent to Event Hub!"
            }catch{
                Write-Error "$($Error[0])"
                Write-Error "Failed to send to Event Hub"
            }
            Write-Verbose "Resetting pagination"
            $paginatedJson = [System.Collections.Concurrent.ConcurrentBag[psobject]]::new()
            $currentSize = 0
        }
        # Add the current item to the current JSON array
        $paginatedJson.Add( $item ) | Out-Null
        $currentSize += $jsonSize
    }

    if ($paginatedJson.Count -gt 0) {
        # Send the POST request
        try{
            Write-Output "Sending Batch to Event Hub with $($currentSize) bytes"
            Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $($paginatedJson | ConvertTo-Json -Depth 100)
            # Output the response
            Write-Host "Event sent to Event Hub!"
        }catch{
            Write-Error "$($Error[0])"
            Write-Error "Failed to send to Event Hub"
        }
    }

    ## Authenticate to ADX
    try {
        Write-Host "Getting Access Token"
        $adxAccessToken = (Get-AzAccessToken -ResourceUrl $clusterUri).Token
        Write-Host "Access Token Generated for $($clusterUri)"
    }
    catch {
        Write-Error "Unable to generate access token for ADX Cluster $($Cluster): $($_.Exception.Message)" -ErrorAction Stop
    }

    $adxAuthHeader = @{
        "Content-Type"  = "application/json"
        "Authorization" = "Bearer $($adxAccessToken)"
        "Host"          = $clusterHost
    }

    ## Automate ADX external table mapping
    $dataTypeMapping = @{
        "Boolean" = "bool"
        "Datetime" = "datetime"
        "Double" = "decimal"
        "Object[]" = "dynamic"
        "PSCustomObject" = "dynamic"
        "HashTable" = "dynamic"
        "Guid" = "guid"
        "Int32" = "long"
        "Int64" = "long"
        "String" = "string"
    }

    $keys = [Collections.Generic.HashSet[string]]@()
    Write-Host "Building Key Mapping for $($table)"
    foreach($result in $Payload){
        #$keyNames = $result | Get-Member -MemberType NoteProperty | Select-Object -ExpandProperty Name
        $keyNames = $result.PSObject.Properties.Name
        foreach($key in $keyNames){
            $keys.Add($key) | Out-Null
        }
    }

    $mappings = [Collections.Generic.HashSet[string]]@() 
    Write-Host "Analysing $($keys.Count) keys"
    foreach($key in $keys){
        $type = $null
        try{
            if(($Payload.$key | Measure-Object).Count -ne 0){
                $type = (($Payload.$key | Sort-Object -Top 1).GetType()).Name
            }elseif($key -like "*dateTime*"){
                $type = "DateTime"
            }else{
                $type = "String"
            }
        }catch{
            Write-Error "$key $($Error[0])"
        }
        $dataType = $dataTypeMapping.($type)
        Write-Host "$($key) is $($type) and remapped to $($dataType)" -ForegroundColor Green
        $mappings.Add("$($key): $($dataType)") | Out-Null
    }

    ## Send Control Command to ADX to create/alter table
    $tableQuery = ".create table $($table.substring(0,1).toupper()+$table.substring(1))(
        $($mappings -join ",")
    )"

    $requestBody = @{
        "db"         = $Database
        "csl"        = $tableQuery.replace("`n","").replace("`r","")
    } | ConvertTo-Json -Depth 100
    
    $requestBody
    $response = Invoke-RestMethod -Uri $queryUri -Method POST -Headers $adxAuthHeader -Body $requestBody -ContentType "application/json"
    Write-Host $response
    
    # $connectionString="managed_identity=system" ### Disabled SAS Based Uploads until further discussion Refer to line 123 for SAS Code
    # Write-Host "Connecting from ADX to Storage Account using connection string: $($connectionString)" -ForegroundColor Cyan
    # $ingestPath = "h@'https://$($storageAccountName).blob.core.windows.net/$($containerName)/$($fileName);$($connectionString)'"
    # $ingestQuery = ".ingest into table $($table.substring(0,1).toupper()+$table.substring(1))(
    #         $($ingestPath)
    #     ) with '{ format : `"multijson`" }'"

    # $ingestBody = @{
    #     "db"         = $Database
    #     "csl"        = $ingestQuery.replace("`n","").replace("`r","")
    # } | ConvertTo-Json -Depth 100
    
    # $ingestBody 
    # $ingestResponse = Invoke-RestMethod -Uri $queryUri -Method POST -Headers $adxAuthHeader -Body $ingestBody -ContentType "application/json"
    # Write-Host $ingestResponse

}
