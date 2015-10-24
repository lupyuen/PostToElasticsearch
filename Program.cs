//  Read a tab-delimited text file and send the fields to Elasticsearch for indexing.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace PostToElasticsearch
{
    class Program
    {
        static void Main(string[] args)
        {
            //  curl -XPOST http://YOUR_ELASTICSEARCH_URL/_bulk --data-binary @log.json
            var url = "http://YOUR_ELASTICSEARCH_URL/_bulk";

            var file = File.OpenText("../../../records.txt");
            for (;;)
            {
                if (file.EndOfStream) break;
                var line = new StringBuilder();
                for (;;)
                {
                    var partialLine = file.ReadLine();
                    line.Append(partialLine);
                    //  Assumes 12 fields per line.
                    if (line.ToString().Split('\t').Length >= 12) break;
                }
                var lineSplit = line.ToString().Split('\t');
                var fields = new Dictionary<string, object>();
                fields["field1"] = lineSplit[0];
                fields["field2"] = lineSplit[1];
                fields["field3"] = lineSplit[2];
                fields["field4"] = lineSplit[3];
                fields["field5"] = lineSplit[4];
                fields["field6"] = lineSplit[5];
                fields["field7"] = lineSplit[6];
                fields["field8"] = lineSplit[7];
                fields["field9"] = lineSplit[8];
                fields["field10"] = lineSplit[9];
                fields["field11"] = lineSplit[10];
                fields["field12"] = lineSplit[11];
                ProcessRecord(url, fields);
            }
            SendToServer(url);
        }

        static void ProcessRecord(string url, Dictionary<string, object> fields)
        {
            var id = Guid.NewGuid().ToString();
            var date = DateTime.UtcNow.ToString("yyyy.MM.dd");
            var timestamp = DateTime.UtcNow.ToString("o");
            var fieldsSerialised = Newtonsoft.Json.JsonConvert.SerializeObject(fields);
            fieldsSerialised = fieldsSerialised.Substring(1, fieldsSerialised.Length - 2);
            Console.WriteLine(fieldsSerialised);

            //var body0 = File.ReadAllText("../../../log.json");
            //@"{""index"":{""_index"":""cwl-2015.10.24"",""_type"":""AWSIotLogs"",""_id"":""32240058904505356328342600332870297578238621653465497603""}}" + "\n" +
            //@"{""date"":""2015-10-24"",""traceid"":""9031c4ed-2fa1-4e4f-8cb2-806642a03802"",""loglevel"":""INFO3"",""principalid"":""5c46ea701ff9cd1ab0fcb52ab5f109f629a3ee029b602792f882a8bf1d56fab2"",""time"":""13:44:01.044"",""event"":""PublishEvent"",""topicname"":""$aws/things/g0_temperature_sensor/shadow/update"",""message"":""PublishIn"",""status"":""SUCCESS"",""device"":""g0_temperature_sensor"",""@id"":""32240058904505356328342600332870297578238621653465497603"",""@timestamp"":""2015-10-24T13:44:01.044Z"",""@message"":""2015-10-24 13:44:01.044 TRACEID:9031c4ed-2fa1-4e4f-8cb2-806642a03802 PRINCIPALID:5c46ea701ff9cd1ab0fcb52ab5f109f629a3ee029b602792f882a8bf1d56fab2 [INFO] EVENT:PublishEvent TOPICNAME:$aws/things/g0_temperature_sensor/shadow/update MESSAGE:PublishIn Status: SUCCESS"",""@owner"":""595779189490"",""@log_group"":""AWSIotLogs"",""@log_stream"":""5c46ea701ff9cd1ab0fcb52ab5f109f629a3ee029b602792f882a8bf1d56fab2""}" + "\n" +
            var body =
                @"{ ""index"":{""_index"":""cwl-%%DATE%%"",""_type"":""LogRecord"",""_id"":""%%ID%%""}}" + "\n" +
                "{" + fieldsSerialised + @",""@id"":""%%ID%%"",""@timestamp"":""%%TIMESTAMP%%""}" + "\n";
            body = body
                .Replace("%%ID%%", id)
                .Replace("%%DATE%%", date)
                .Replace("%%TIMESTAMP%%", timestamp)
                ;
            buffer.Append(body);
            if (buffer.Length > 8192) SendToServer(url);
        }

        static StringBuilder buffer = new StringBuilder();

        static void SendToServer(string url)
        {
            var result = SendPostRequest(url, null, buffer.ToString());
            Console.WriteLine(result);
            buffer.Clear();
        }

        public static string SendPostRequest(string url, string contentType, string body, bool async = false)
        {
            // Create a request using a URL that can receive a post. 
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            //  Allow zip compression.
            request.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
            // Set the Method property of the request to POST.
            request.Method = "POST";
            // Create POST data and convert it to a byte array.
            byte[] byteArray = Encoding.UTF8.GetBytes(body);
            // Set the ContentType property of the WebRequest.
            if (contentType != null) request.ContentType = contentType;
            // Set the ContentLength property of the WebRequest.
            request.ContentLength = byteArray.Length;

            //  Add headers:
            //  'Token': token,
            //  'Cookie': cookieName + '=' + cookie
            WebHeaderCollection myWebHeaderCollection = request.Headers;

            // Get the request stream.
            Stream dataStream = request.GetRequestStream();

            // Write the data to the request stream.
            dataStream.Write(byteArray, 0, byteArray.Length);
            // Close the Stream object.
            dataStream.Close();

            string result = null;
            if (async)
            {
                //  Don't wait for the response.
                request.GetResponseAsync();
                result = "async";
            }
            else
            {
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                // Display the status.

                //Console.WriteLine (((HttpWebResponse)response).StatusDescription);
                // Get the stream containing content returned by the server.
                dataStream = response.GetResponseStream();
                // Open the stream using a StreamReader for easy access.
                StreamReader reader = new StreamReader(dataStream);
                // Read the content.
                string responseFromServer = reader.ReadToEnd();
                // Display the content.
                //Console.WriteLine (responseFromServer);
                // Clean up the streams.
                reader.Close();
                dataStream.Close();
                response.Close();
                result = responseFromServer;
            }
            return result;
        }

    }
}
