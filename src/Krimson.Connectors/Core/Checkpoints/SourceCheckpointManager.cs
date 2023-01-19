using Krimson.Readers;
using static Serilog.Log;

namespace Krimson.Connectors.Checkpoints;

public delegate ValueTask<SourceCheckpoint> LoadCheckPoint(string topic, CancellationToken cancellationToken);

[PublicAPI]
public class SourceCheckpointManager {
    static readonly Serilog.ILogger Log = ForContext<SourceCheckpointManager>();
    
    public SourceCheckpointManager(LoadCheckPoint loadCheckPoint) {
        LoadCheckPoint = loadCheckPoint;
        Checkpoints    = new();
    }

    public SourceCheckpointManager(KrimsonReader reader)  {
        LoadCheckPoint = (topic, ct) => reader.LastRecords(topic, ct).AsAsyncEnumerable()
            .Select(x => new SourceCheckpoint(x.Id, x.Timestamp.UnixTimestampMs))
            .DefaultIfEmpty(SourceCheckpoint.None)
            .MaxAsync(ct);
        
        Checkpoints = new();
    }

    LoadCheckPoint                       LoadCheckPoint { get; }
    Dictionary<string, SourceCheckpoint> Checkpoints    { get; }

    public async ValueTask<SourceCheckpoint> GetCheckpoint(string topic, CancellationToken cancellationToken = default) {
        if (Checkpoints.TryGetValue(topic, out var checkpoint))
            return checkpoint;
        
        var loadedCheckpoint = await LoadCheckPoint(topic, cancellationToken).ConfigureAwait(false);

        if (loadedCheckpoint == SourceCheckpoint.None) {
            Log.Information("Checkpoint not found");
        }
        else {
            Log.Information(
                "Checkpoint loaded: {EventTime} {Topic} [{Partition}] @ {Offset}",
                loadedCheckpoint.Timestamp, loadedCheckpoint.RecordId.Topic, 
                loadedCheckpoint.RecordId.Partition, loadedCheckpoint.RecordId.Offset 
            );
        }
        
        return Checkpoints[topic] = loadedCheckpoint;
    }
    
    public SourceCheckpoint TrackCheckpoint(SourceCheckpoint checkpoint) {
        var actualCheckpoint = Checkpoints[checkpoint.RecordId.Topic];

        if (checkpoint == SourceCheckpoint.None || actualCheckpoint.Timestamp >= checkpoint.Timestamp) {
            Log.Debug(
                "Checkpoint ignored: {EventTime} {Topic} [{Partition}] @ {Offset}",
                checkpoint.Timestamp, checkpoint.RecordId.Topic, 
                checkpoint.RecordId.Partition, checkpoint.RecordId.Offset 
            );
            
            return checkpoint;
        }

        Checkpoints[checkpoint.RecordId.Topic] = checkpoint;
        
        Log.Information(
            "Checkpoint tracked: {EventTime} {Topic} [{Partition}] @ {Offset}",
            checkpoint.Timestamp, checkpoint.RecordId.Topic, 
            checkpoint.RecordId.Partition, checkpoint.RecordId.Offset 
        );

        return checkpoint;
    }
}