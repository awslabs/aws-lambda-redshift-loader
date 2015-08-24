Param(
	[switch] $deploy,
	[string] $functionname =  $( if($deploy.IsPresent) {Read-Host "Your Lambda function name: " }),
	[string] $role_arn =  $( if($deploy.IsPresent) {Read-Host "The Role ARN your function runs as: " })
)
install-module pscx

$version = (Get-content package.json -raw | ConvertFrom-Json).version

Write-Zip -Path  index.js,common.js,constants.js, kmsCrypto.js, upgrades.js, *.txt, package.json, node_modules/, -OutputPath .\dist\AWSLambdaRedshiftLoader-$version.zip



if($deploy.IsPresent) {
	$zipFilePath = Resolve-Path "dist\AWSLambdaRedshiftLoader-$version.zip"

	Try
	{
		$deployedFn =  Get-LMFunction -FunctionName $functionname
		"Function Exists - trying to update"
		try{
			[system.io.stream]$zipStream = [system.io.File]::OpenRead($zipFilePath)
			$ms = new-Object IO.MemoryStream
			$zipStream.CopyTo($ms);
			Update-LMFunctionCode -FunctionName $functionname -ZipFile $ms
		}
		catch{
			$ErrorMessage = $_.Exception.Message
			Write-Host $ErrorMessage
			Write-Host  $_.Exception
			break
		}

	}
	Catch [InvalidOperationException]{
		"Function is New - trying to Publish"
		Publish-LMFunction -FunctionName $functionname -FunctionZip $zipFilePath -Handler "index.handler" `
			 -Runtime nodejs -Role $role_arn -Description "loads the unzipped csv's from billingUnzip into redshift tables" `
	 -MemorySize 128 -Timeout 60 -region us-west-2
	}
}