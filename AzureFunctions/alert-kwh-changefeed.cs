#r "Microsoft.Azure.DocumentDB.Core"
#r "Newtonsoft.Json"
 
using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Extensions.Logging;
 
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text;
using Newtonsoft.Json;
 
public static void Run(IReadOnlyList<Document> input, IAsyncCollector<objAlert> outputDocument, ILogger log)
{
    if (input != null && input.Count > 0)
    {
    log.LogInformation("Documents modified " + input.Count);
    log.LogInformation("First document Id " + input[0].Id);
    }
    
    var databasename = Environment.GetEnvironmentVariable("databasename");
    var collectionname = Environment.GetEnvironmentVariable("collectionname");
    
    double usage_kwh = 0;
    double kwh_upperbound = 0;
    var ESIID = "10089010119004454XXXXX";
    var entitytype = "kwh_upperbound";
    var entitytyperaw = "kwh_raw";
    
    Int32 unixTimestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
    Int32 unixTimestampCentralTime = unixTimestamp - 21600;
    DateTime sTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    var ddtcentral = sTime.AddSeconds(unixTimestampCentralTime);
    
    log.LogInformation("ddtcentral: " + ddtcentral);
    
    var endpoint = Environment.GetEnvironmentVariable("endpoint");
    var masterKey = Environment.GetEnvironmentVariable("masterKey");
    FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true };
    using (var client = new DocumentClient(new Uri(endpoint), masterKey)) 
    {
    IQueryable<dynamic> qryUpperbound = client.CreateDocumentQuery<dynamic>(
    UriFactory.CreateDocumentCollectionUri(databasename, collectionname), "SELECT VALUE c.kwh_upperbound FROM c Where c.entitytype='" + entitytype + "'", queryOptions);
    
    foreach (dynamic dynDoc in qryUpperbound)
    {
    kwh_upperbound = dynDoc;
    log.LogInformation("set kwh_upperbound: " + kwh_upperbound.ToString()); 
    }
    } 
    
    foreach(var changeInput in input)
    {
    if (changeInput.GetPropertyValue <string>("entitytype") == entitytyperaw)
    {
    //log.LogInformation("hello world!"); 
    
    usage_kwh = changeInput.GetPropertyValue <double>("usage_kwh");
    log.LogInformation("entitytype: " + changeInput.GetPropertyValue <string>("entitytype"));
    log.LogInformation("usage_kwh: " + usage_kwh);
    
    if (usage_kwh > kwh_upperbound)
    {
    log.LogInformation("HIGH");
    
    var myjson = "{'ESIID':'" + ESIID + "'" + 
    ",'entitytype':'kwh_alert_high'" +
    ",'dt_tm':'" + ddtcentral + "'" + 
    ",'usage_kwh':" + usage_kwh + 
    ",'kwh_upperbound':" + kwh_upperbound +
    "}";
    
    objAlert document = JsonConvert.DeserializeObject<objAlert>(myjson);
    outputDocument.AddAsync(document);
    log.LogInformation("HIGH. " + myjson); 
    }
    else 
    {
    log.LogInformation("NORMAL");
    
    var myjson = "{'ESIID':'" + ESIID + "'" + 
    ",'entitytype':'kwh_alert_normal'" +
    ",'dt_tm':'" + ddtcentral + "'" + 
    ",'usage_kwh':" + usage_kwh + 
    ",'kwh_upperbound':" + kwh_upperbound +
    "}"; 
    
    objAlert document = JsonConvert.DeserializeObject<objAlert>(myjson);
    outputDocument.AddAsync(document);
    log.LogInformation("NORMAL. " + myjson); 
    }
    }
    }
}
 
public class objAlert
{
 public string ESIID { get; set; }
 public string dt_tm { get; set; }
 public double usage_kwh { get; set; }
 public double upperbound_kwh { get; set; }
 public string entitytype { get; set; }
}
