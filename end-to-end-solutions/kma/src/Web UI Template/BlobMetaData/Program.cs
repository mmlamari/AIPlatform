
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;


namespace BlobCopyOneStorageToAnother
{
    class Program
    {

        static CloudStorageAccount  sourceStorageConnectionString;
        static CloudStorageAccount destinationStorageConnectionString;
        static CloudBlobClient sourceCloudBlobClient;
        static CloudBlobClient targetCloudBlobClient;
        static string path = @"C:\test.csv";
        static async Task Main(string[] args)
        {

          var builder = new ConfigurationBuilder()
       .SetBasePath(Directory.GetCurrentDirectory())
       .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

             Console.WriteLine("Azure BlobCopyOneStorageToAnother started\n");

            string AzureBlobStorageSourceDev = configuration.GetConnectionString("AzureBlobStorageSourceProd");
            string AzureBlobStorageTargetDev = configuration.GetConnectionString("AzureBlobStorageTargetProd");
            //Get source and destination azure storage account connection string from app.config
            sourceStorageConnectionString = CloudStorageAccount.Parse(AzureBlobStorageSourceDev);
            destinationStorageConnectionString = CloudStorageAccount.Parse(AzureBlobStorageTargetDev);

            sourceCloudBlobClient = sourceStorageConnectionString.CreateCloudBlobClient();
            targetCloudBlobClient = destinationStorageConnectionString.CreateCloudBlobClient();

              
            string targetblob = "rawdata";
           // await CopyBlobsAsync(sourceCloudBlobClient, targetCloudBlobClient, "sbapcd", targetblob);
            await CopyBlobsAsync(sourceCloudBlobClient, targetCloudBlobClient, "scaqmd", targetblob);
            //await CopyBlobsAsync(sourceCloudBlobClient, targetCloudBlobClient, "smaqmd", targetblob);


            Console.WriteLine("All blob copied has been successful:{0}", targetblob);
            Console.Read();
            //Get source and destination container name from app.config

        }

        public  static async Task CopyBlobsAsync(CloudBlobClient sourceCloudBlobClient ,CloudBlobClient targetCloudBlobClient, string sourceblob , string targetblob  )
        {

            CloudBlobContainer sourceContainer = sourceCloudBlobClient.GetContainerReference(sourceblob);
            CloudBlobContainer destinationContainer = targetCloudBlobClient.GetContainerReference(targetblob);

            //Create container into blob if not exists
            await destinationContainer.CreateIfNotExistsAsync();

            Console.WriteLine("Started copying all blob: " + sourceContainer.Name + "  to " + destinationContainer.Name);
            BlobResultSegment segment = await sourceContainer.ListBlobsSegmentedAsync(null);
            List<IListBlobItem> list = new List<IListBlobItem>();
            list.AddRange(segment.Results);
            // Copy each blob 

            var numbercopies = 10000;
            foreach (IListBlobItem blob in list)
            {

                if(numbercopies > 0)
                { 
                //Get blob url 
                Uri thisBlobUri = blob.Uri;

                //Get blob name 
                var blobName = Path.GetFileName(thisBlobUri.ToString());
                Console.WriteLine("Copying blob: " + blobName);


                CloudBlockBlob sourceBlob = sourceContainer.GetBlockBlobReference(blobName);
                CloudBlockBlob targetBlob = destinationContainer.GetBlockBlobReference(blobName);

               
                var b_metadata = new blobmetadata();
              
                b_metadata.readmetadata(path);


                    if (b_metadata.listA.ContainsKey(targetBlob.Name))
                    {

                        var file_url = String.IsNullOrEmpty(b_metadata.listA[targetBlob.Name].file_url) ? "NA" : b_metadata.listA[targetBlob.Name].file_url;
                        var last_update = String.IsNullOrEmpty(b_metadata.listA[targetBlob.Name].last_update) ? "NA" : b_metadata.listA[targetBlob.Name].last_update;
                        var air_district = String.IsNullOrEmpty(b_metadata.listA[targetBlob.Name].air_district) ? "NA" : b_metadata.listA[targetBlob.Name].air_district;

                        if (targetBlob.Metadata.ContainsKey("file_url")) 
                                targetBlob.Metadata["file_url"] = file_url;
                            else
                            targetBlob.Metadata.Add("file_url", file_url);


                        if (targetBlob.Metadata.ContainsKey("last_update"))
                            targetBlob.Metadata["last_update"] = last_update;
                        else
                            targetBlob.Metadata.Add("last_update", last_update);


                        if (targetBlob.Metadata.ContainsKey("airdistrict"))
                            targetBlob.Metadata["airdistrict"] = air_district;
                        else
                            targetBlob.Metadata.Add("airdistrict", air_district);


                     //Task task = TransferManager.CopyAsync(sourceBlob, targetBlob, true /* isServiceCopy */);
                      await targetBlob.SetMetadataAsync();
                    }
                   
                   

                }
                else break;

                --numbercopies;

                // //copy blob frim source to destination

            }

    


        }







    }

   
    public class blobmetadata
    {
        //file_url,last_update,pdf_file_name_w_ext,pdf_file_name_wout_ext,air_district
      //  public string blobname;
        public string file_url;
        public string last_update;
        public string pdf_file_name_w_ext;
        public string air_district;
        // public List<blobmetadata> listA = new List<blobmetadata>();
        public Dictionary<string, blobmetadata> listA = new Dictionary<string, blobmetadata>();
        public void readmetadata(string path)
        {

            using (var reader = new StreamReader(path ))
            {
                
               
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var linemeatadata = new blobmetadata();
                    var values = line.Split(',');

                    linemeatadata.file_url = values.Length <= 0 ? "" : values[0];
                    linemeatadata.last_update = values.Length <= 2 ? "" : values[1] + "," + values[2];//date time issue
                    linemeatadata.pdf_file_name_w_ext = values.Length <= 3 ? "" : values[3];
                    linemeatadata.air_district = values.Length <= 5 ? "" : values[5];
                     
                    if(!listA.ContainsKey(linemeatadata.pdf_file_name_w_ext))
                    listA.Add(linemeatadata.pdf_file_name_w_ext , linemeatadata);




                }
            }



        }

    }
}