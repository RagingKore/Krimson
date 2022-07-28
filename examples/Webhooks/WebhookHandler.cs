using System.Text.Json.Nodes;
using Google.Protobuf.WellKnownTypes;
using Krimson.Connectors;
using Krimson.Connectors.Http;
using Microsoft.AspNetCore.Http;

public class WebhookHandler : IWebhookHandler {
    public async Task<bool> ValidatePayload(HttpContext context) {
        var header = context.Request.Headers["X-Signature"].ToString();
        return header == "thisisfine";
    }

    public async Task<IEnumerable<SourceRecord>> GetData(JsonNode node) {
        var meters = node["meters"]?.AsArray();

        var list = new List<SourceRecord>();

        foreach (var meter in meters) {
            var json = meter.ToJsonString();

            try {
                var recordId  = node["id"]!.GetValue<string>();
                var timestamp = Timestamp.FromDateTimeOffset(node["last_modified"]!.GetValue<DateTimeOffset>());
                var data      = Struct.Parser.ParseJson(json);

                list.Add(
                    new SourceRecord {
                        Id        = recordId,
                        Data      = data,
                        Timestamp = timestamp,
                        Type      = "metering-point",
                        Operation = SourceOperation.Snapshot
                    }
                );
            }
            catch (Exception ex) {
                //Log.ForContext("JsonData", json, true).Error(ex, "Failed to parse source record");
                list.Add(SourceRecord.Empty);
            }
        }

        return list;
    }
}