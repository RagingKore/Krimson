using Krimson.Readers;

namespace Krimson.Connectors;

public static class KrimsonReaderCheckpointExtensions {
    public static async ValueTask<Checkpoint> LoadCheckpoint(this KrimsonReader reader, string? topic, CancellationToken cancellationToken) {
        if (topic is null)
            return Checkpoint.None;

        var records = new List<KrimsonRecord>();

        await foreach (var record in reader.LastRecords(topic!, cancellationToken).ConfigureAwait(false))
            records.Add(record);

        var checkpoint = records
            .Select(x => new Checkpoint(x.Id, ((SourceRecord)x.Value).Timestamp))
            .MaxBy(x => x);

        return checkpoint ?? Checkpoint.None;
    }
}