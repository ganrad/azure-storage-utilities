// 
// Copyright (c) Microsoft.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
using Azure.Core;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;

using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;

/**
 * Description:
 * This program updates access tiers for Azure Storage Blobs. Uses Azure Storage .NET Core v12 API.
 *
 * Author: @Microsoft
 * Dated: 06-15-2020
 *
 * NOTES: Capture updates to the code below.
 */
namespace ChangeBlobTiers
{
    class Program
    {
	static int BatchSize = 50; // Set the Batch Size, 256 is max.
	// Access tiers can be => Hot, Cool, Archive
	static AccessTier SourceTier = AccessTier.Hot;  // Set the Source Access Tier
	static AccessTier TargetTier = AccessTier.Cool; // Set the Target Access Tier
	static string AccountName = "sourcecms"; // Set the Storage account name
	static string AccountKey = Environment.GetEnvironmentVariable("AZURE_STORAGE_ACCOUNT_KEY"); // Set the Storage account key via env. variable
	static string ContainerName = "case-01"; // Set the source blob container name
	static int SasKeyExpiryTime = 1; // Default 1 hour

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Blob operations");

	    if ( String.IsNullOrEmpty(AccountKey) )
	    {
		Console.WriteLine("The environment variable: AZURE_STORAGE_ACCOUNT_KEY, is not set!");
		Environment.Exit(-1);
	    };

	    if ( SourceTier.Equals(TargetTier) )
	    {
	       Console.WriteLine("Source and Target Access Tiers cannot be the same!");

	       Environment.Exit(-1);
	    };

	    Stopwatch stopWatch = new Stopwatch();
	    try
	    {
		// Start the stop watch
		stopWatch.Start();

	        BlobServiceClient blobSvcClient = SharedAccessSignatureAuthAsync();
		BlobContainerClient contClient = blobSvcClient.GetBlobContainerClient(ContainerName);

		Uri[] blobUrls = new Uri[BatchSize];
		int batchCount = 0;
		int totalProcessed = 0;
		List<Task<Azure.Response[]>> taskList = new List<Task<Azure.Response[]>>();

		BlobClient blobClient = null;
		IDictionary<string,string> dict = null;
	        Console.WriteLine("Enumerating blobs ...");
	        await foreach (BlobItem blobItem in contClient.GetBlobsAsync(BlobTraits.Metadata))
	        {
		    Console.WriteLine("-------------------------");
		    Console.WriteLine("\tName: " + blobItem.Name);
		    blobClient = contClient.GetBlobClient(blobItem.Name);
		    Console.WriteLine("\tUri: " + blobClient.Uri);

		    BlobItemProperties props = blobItem.Properties;
		    
		    AccessTier ?atier = props.AccessTier;
		    Console.WriteLine("\tAccess Tier: " + atier);
		    if ( atier.Equals(SourceTier) )
		    {
		       blobUrls[batchCount] = blobClient.Uri;
		       batchCount++;
		    };

		    dict = blobItem.Metadata;
		    if ( (dict != null) && (dict.Count > 0) )
		    {
		      foreach (KeyValuePair<string,string> item in dict)
		        Console.WriteLine("Key: {0}, Value: {1}", item.Key, item.Value);
		    }
		    else
		      Console.WriteLine("\tNo metadata for this blob item");

		    if ( batchCount == BatchSize )
		    {
		       BlobBatchClient batch = blobSvcClient.GetBlobBatchClient();
		       taskList.Add(batch.SetBlobsAccessTierAsync(blobUrls, TargetTier));
		       totalProcessed += batchCount;
		       batchCount = 0;

		       Console.WriteLine("-------------------------");
		       Console.WriteLine($"No. of Blobs moved to {TargetTier} tier: {totalProcessed}");
		    };
	        }

		if ( batchCount > 0 )
		{
		   BlobBatchClient batch = blobSvcClient.GetBlobBatchClient();
		   taskList.Add(batch.SetBlobsAccessTierAsync(blobUrls, TargetTier));
		   totalProcessed += batchCount;
		};

		if ( taskList.Count > 0 )
		   Task.WaitAll(taskList.ToArray()); // Wait for all Tasks to finish!

		// Stop the timer
		stopWatch.Stop();
		TimeSpan ts = stopWatch.Elapsed;
		string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}",
		   ts.Hours, ts.Minutes, ts.Seconds);
		Console.WriteLine("------------------------------------");
		Console.WriteLine($"Moved {totalProcessed} Blobs from {SourceTier} to {TargetTier} tier.");
		Console.WriteLine($"Total Runtime: {elapsedTime}");
		Console.WriteLine("------------------------------------");
	    }
	    catch (Exception ex)
	    {
	       Console.WriteLine("Encountered exception: " + ex);
	    }
        }

	private static BlobServiceClient SharedAccessSignatureAuthAsync()
	{
	   // Create a service level SAS that only allows reading from service
	   // level APIs
	   AccountSasBuilder sas = new AccountSasBuilder
	   {
	      // Allow access to blobs
	      Services = AccountSasServices.Blobs,

	      // Allow access to all service level APIs
	      ResourceTypes = AccountSasResourceTypes.All,
	      
	      // Specify token expiration in hours
	      ExpiresOn = DateTimeOffset.UtcNow.AddHours(SasKeyExpiryTime)
	   };
	   
	   // Allow all access => Create, Delete, List, Process, Read, Write & Update
	   sas.SetPermissions(AccountSasPermissions.All);
	   
	   // Create a SharedKeyCredential that we can use to sign the SAS token
	   StorageSharedKeyCredential credential = 
	      new StorageSharedKeyCredential(AccountName, AccountKey);
	   
	   // Build a SAS URI
	   string storageAccountUri = String.Format("https://{0}.blob.core.windows.net",AccountName);
	   UriBuilder sasUri = new UriBuilder(storageAccountUri);
	   sasUri.Query = sas.ToSasQueryParameters(credential).ToString();
	    
	   // Create and return a service client that can authenticate with the SAS URI
	   return new BlobServiceClient(sasUri.Uri);
	}
    }
}
