#r "Microsoft.Azure.DocumentDB.Core"
#r "Newtonsoft.Json"
 
using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
 
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text;
using Newtonsoft.Json;
//public static void Run(TimerInfo myTimer, ILogger log)
public static void Run(TimerInfo myTimer, IAsyncCollector<objAgg> outputDocument, ILogger log)
{
    log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

     //Update the db and collection name   
    //string databasename = "db-cosmos-workshop";
    //string collectionname = "weather-utility";
    var databasename = Environment.GetEnvironmentVariable("databasename");
    var collectionname = Environment.GetEnvironmentVariable("collectionname");    
    var ESIID = "10089010119004454XXXXX";
    var entitytype = "kwh_raw";
    double myavg = 0;
    double mymax = 0;
    double mymin = 0;
    
    log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
    
    Int32 unixTimestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
    Int32 unixTimestamp2 = unixTimestamp - 180;
    Int32 unixTimestampCentralTime = unixTimestamp - 21600;
    
    log.LogInformation("unixTimestamp2: " + unixTimestamp2);
    
    DateTime sTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    var ddt = sTime.AddSeconds(unixTimestamp);
    var ddt2 = sTime.AddSeconds(unixTimestamp2);
    var ddtcentral = sTime.AddSeconds(unixTimestampCentralTime);
    
    log.LogInformation("date time : " + ddt);
    log.LogInformation("date time 2: " + ddt2);
    
    var endpoint = Environment.GetEnvironmentVariable("endpoint");
    var masterKey = Environment.GetEnvironmentVariable("masterKey");
    
    FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true };
    
    using (var client = new DocumentClient(new Uri(endpoint), masterKey)) 
    { 
    
    IQueryable<dynamic> qryKwhAvg = client.CreateDocumentQuery<dynamic>(
    UriFactory.CreateDocumentCollectionUri(databasename, collectionname), "SELECT VALUE AVG(c.usage_kwh) FROM c WHERE c.entitytype='" + entitytype + "' AND c._ts > " + unixTimestamp2, queryOptions);
    //log.LogInformation("sql for avg: SELECT VALUE AVG(c.usage_kwh) FROM c WHERE c.entitytype='" + entitytype + "' AND c._ts > " + unixTimestamp2);
    
    IQueryable<dynamic> qryKwhMax = client.CreateDocumentQuery<dynamic>(
    UriFactory.CreateDocumentCollectionUri(databasename, collectionname), "SELECT VALUE MAX(c.usage_kwh) FROM c WHERE c.entitytype='" + entitytype + "' AND c._ts > " + unixTimestamp2, queryOptions);
    
    IQueryable<dynamic> qryKwhMin = client.CreateDocumentQuery<dynamic>(
    UriFactory.CreateDocumentCollectionUri(databasename, collectionname), "SELECT VALUE MIN(c.usage_kwh) FROM c WHERE c.entitytype='" + entitytype + "' AND c._ts > " + unixTimestamp2, queryOptions);
    
    foreach (dynamic dynDoc in qryKwhAvg)
    {
    myavg = dynDoc;
    myavg = Math.Round(myavg, 4);
    log.LogInformation("AVG(usage_kwh): " + myavg.ToString()); 
    }
    
    foreach (dynamic dynDoc in qryKwhMax)
    {
    mymax = dynDoc;
    mymax = Math.Round(mymax, 4);
    log.LogInformation("MAX(usage_kwh): " + mymax.ToString()); 
    }
    
    foreach (dynamic dynDoc in qryKwhMin)
    {
    mymin = dynDoc;
    mymin = Math.Round(mymin, 4);
    log.LogInformation("MIN(usage_kwh): " + mymin.ToString()); 
    }
    
    var myjson = "{'ESIID':'" + ESIID + "'" + 
    ",'entitytype':'kwh_agg'" +
    ",'dt_tm':'" + ddtcentral + "'" + 
    ",'avg_kwh':" + myavg + 
    ",'max_kwh':" + mymax +
    ",'min_kwh':" + mymin +
    "}"; 
    
    objAgg document = JsonConvert.DeserializeObject<objAgg>(myjson);
    outputDocument.AddAsync(document);
    log.LogInformation("document created. " + myjson); 
    }
}

public class objAgg
{
 public string ESIID { get; set; }
 public string dt_tm { get; set; }
 public double avg_kwh { get; set; }
 public double max_kwh { get; set; }
 public double min_kwh { get; set; }
 public string entitytype { get; set; }
}
