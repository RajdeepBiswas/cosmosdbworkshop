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
 
public static void Run(IReadOnlyList<Document> input, IAsyncCollector<objAgg> outputDocument, ILogger log)
{
 if (input != null && input.Count > 0)
 {
 log.LogInformation("Documents modified " + input.Count);
 log.LogInformation("First document Id " + input[0].Id);
 }
 
 var entitytype = "weather_raw";
 var entitytypeout = "weather_agg";
 var databasename = Environment.GetEnvironmentVariable("databasename");
 var collectionname = Environment.GetEnvironmentVariable("collectionname");
 
 var ESIID = "10089010119004454XXXXX";
 
 double myavg = 0;
 
 DateTime dt_tm;
 
 int dt_min;
 double modresult = 0;
 double Temp_F = 0;
 
 Int32 unixTimestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
 Int32 unixTimestamp2 = unixTimestamp - 300; // 5 minutes back
 Int32 unixTimestampCentralTime = unixTimestamp - 21600;
 
 log.LogInformation("unixTimestamp2: " + unixTimestamp2);
 
 DateTime sTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
 var ddt = sTime.AddSeconds(unixTimestamp);
 var ddt2 = sTime.AddSeconds(unixTimestamp2);
 var ddtcentral = sTime.AddSeconds(unixTimestampCentralTime);
 
 foreach(var changeInput in input)
 {
 if (changeInput.GetPropertyValue <string>("entitytype") == entitytype)
 {
 
 dt_tm = DateTime.Parse(changeInput.GetPropertyValue <string>("dt_tm"));
 
 log.LogInformation("entitytype: " + changeInput.GetPropertyValue <string>("entitytype"));
 log.LogInformation("dt_tm: " + changeInput.GetPropertyValue <string>("dt_tm"));
 log.LogInformation("minute: " + dt_tm.Minute);
 
 dt_min = dt_tm.Minute;
 modresult = dt_min % 5;
 log.LogInformation("modresult: " + modresult);
 
 var endpoint = Environment.GetEnvironmentVariable("endpoint");
 var masterKey = Environment.GetEnvironmentVariable("masterKey");
  
 FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1, EnableCrossPartitionQuery = true };
 
 using (var client = new DocumentClient(new Uri(endpoint), masterKey)) 
 { 
 
 log.LogInformation("using client *");
 log.LogInformation("sql: SELECT VALUE AVG(c.Temp_F_double) FROM c Where c.Temp_F_double >0 AND c.entitytype='" + entitytype + "' AND c._ts > " + unixTimestamp2);
 
 IQueryable<dynamic> qryAvg = client.CreateDocumentQuery<dynamic>(
 UriFactory.CreateDocumentCollectionUri(databasename, collectionname), "SELECT VALUE AVG(c.Temp_F_double) FROM c Where c.Temp_F_double > 0 AND c.entitytype='" + entitytype + "' AND c._ts > " + unixTimestamp2, queryOptions);
 
 foreach (dynamic dynDoc in qryAvg)
 {
 //dynDoc is not null)
 myavg = dynDoc;
 myavg = Math.Round(myavg, 4);
 log.LogInformation("AVG(Temp_F_double): " + myavg.ToString()); 
 }
 
 var myjson = "{'ESIID':'" + ESIID + "'" + 
 ",'entitytype':'" + entitytypeout + "'" +
 ",'dt_tm':'" + ddtcentral + "'" + 
 ",'avg_temp_f':" + myavg +  
 "}";
 
 log.LogInformation("myjson: " + myjson); 
 
 objAgg document = JsonConvert.DeserializeObject<objAgg>(myjson);
 outputDocument.AddAsync(document);
 log.LogInformation("document created. " + myjson); 
 
 }
 
 }
 }
}
 
public class objAgg
{
 public string ESIID { get; set; }
 public string dt_tm { get; set; }
 public double avg_temp_f { get; set; }
 public string entitytype { get; set; }
}
