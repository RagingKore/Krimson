using Krimson.Readers;

namespace Krimson.Connectors.Checkpoints;

public static class KrimsonReaderCheckpointExtensions {
    public static async ValueTask<SourceCheckpoint> LoadCheckpoint(this KrimsonReader reader, string? topic, CancellationToken cancellationToken) {
        if (topic is null)
            return SourceCheckpoint.None; // maybe its an error

        var records = new List<KrimsonRecord>();

        await foreach (var record in reader.LastRecords(topic, cancellationToken).ConfigureAwait(false))
            records.Add(record);

        var checkpoint = records
            .Select(x => new SourceCheckpoint(x.Id, x.Timestamp.UnixTimestampMs))
            .MaxBy(x => x);

        return checkpoint ?? SourceCheckpoint.None;
    }
}