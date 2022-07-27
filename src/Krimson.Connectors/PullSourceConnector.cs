using Krimson.Producers;
using Krimson.Readers;
using static Serilog.Core.Constants;
using static Serilog.Log;
using ILogger = Serilog.ILogger;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace Krimson.Connectors;

[PublicAPI]
public abstract class PullSourceConnector : IPullSourceConnector {
    static readonly Timestamp DefaultCheckpoint = Timestamp.FromDateTimeOffset(DateTimeOffset.MinValue);

    protected PullSourceConnector(KrimsonProducer producer, KrimsonReader reader) {
        Producer = producer;
        Reader   = reader;

        Log        = ForContext(SourceContextPropertyName, GetType().Name);
        State      = new Dictionary<string, SourceRecord>();
        Checkpoint = DefaultCheckpoint;
        IsFirstRun = true;
        IsBusy     = new InterlockedBoolean();
    }

    protected ILogger Log { get; }

    KrimsonProducer                  Producer { get; }
    KrimsonReader                    Reader   { get; }
    Dictionary<string, SourceRecord> State    { get; }

    Timestamp          Checkpoint { get; set; }
    InterlockedBoolean IsBusy     { get; set; }
    bool               IsFirstRun { get; set; }

    public virtual async Task Execute(CancellationToken stoppingToken) {
        try {
            if (IsBusy.EnsureCalledOnce())
                return;
            
            if (IsFirstRun) {
                Checkpoint = await LoadCheckpoint(stoppingToken).ConfigureAwait(false);
                IsFirstRun = false;
                Log.Information("starting from checkpoint {Checkpoint}", Checkpoint.ToDateTimeOffset());
            }

            var unseenRecords = PullRecords(stoppingToken)
                .Where(record => !record.Equals(SourceRecord.Empty))
                .WhereAwaitWithCancellation((record, ct) => FilterRecord(record, Checkpoint, ct))
                .Select(
                    record => State.Any()
                        ? State.ContainsKey(record.Id)
                            ? record.With(x => x.Operation = SourceOperation.Update)
                            : record.With(x => x.Operation = SourceOperation.Insert)
                        : record
                )
                .OrderBy(r => r.Timestamp)
                .WithCancellation(stoppingToken)
                .ConfigureAwait(false);

            var recordCount = 0;

            Timestamp? lastTimestamp = null;

            await foreach (var record in unseenRecords) {
                await DispatchRecord(Producer, record, stoppingToken).ConfigureAwait(false);
                lastTimestamp = record.Timestamp;
                recordCount++;
            }

            if (lastTimestamp is not null) {
                Checkpoint = lastTimestamp;
                
                Log.Information(
                    "checkpoint tracked at {Checkpoint} after pulling {RecordCount} records from source", 
                    Checkpoint.ToDateTimeOffset(), recordCount
                );
            }
            else
                Log.Debug("no unseen records available from source");
        }
        catch (OperationCanceledException) {
            // be kind and don't crash 
        }
        catch (Exception ex) {
            Log.Fatal(ex, "Failed to pull records!");
        }
        finally {
            IsBusy.Set(false);
        }
    }

    public virtual IAsyncEnumerable<SourceRecord> LoadState(CancellationToken cancellationToken = default) => AsyncEnumerable.Empty<SourceRecord>();

    public virtual async ValueTask<Timestamp> LoadCheckpoint(CancellationToken cancellationToken = default) {
        if (Producer.Topic is null)
            return DefaultCheckpoint;

        var records = new List<KrimsonRecord>();

        await foreach (var record in Reader.LastRecords(Producer.Topic!, cancellationToken).ConfigureAwait(false)) {
            Log.Information("last found record {RecordId} {Timestamp}", record.Id, ((SourceRecord)record.Value).Timestamp.ToDateTimeOffset());
            records.Add(record);
        }
       
        var checkpoint = records
            .Select(x => ((SourceRecord)x.Value).Timestamp)
            .MaxBy(x => x);

        return checkpoint ?? DefaultCheckpoint;
    }

    public abstract IAsyncEnumerable<SourceRecord> PullRecords(CancellationToken cancellationToken = default);

    public virtual ValueTask<bool> FilterRecord(SourceRecord record, Timestamp checkpoint, CancellationToken cancellationToken = default) {
        return ValueTask.FromResult(record.Timestamp > checkpoint);
    }

    public virtual async ValueTask<ProducerResult> DispatchRecord(KrimsonProducer producer, SourceRecord record, CancellationToken cancellationToken = default) =>
        await producer.Produce(record, record.Id).ConfigureAwait(false);
}

[PublicAPI]
public class PeriodicSourceConnectorOptions {
    public static readonly TimeSpan DefaultBackoffTime = TimeSpan.FromSeconds(30);

    public TimeSpan BackoffTime { get; set; } = DefaultBackoffTime;
}

[PublicAPI]
public abstract class PeriodicPullSourceConnector : PullSourceConnector {
    protected PeriodicPullSourceConnector(PeriodicSourceConnectorOptions options, KrimsonProducer producer, KrimsonReader reader)
        : base(producer, reader) =>
        Options = options;

    PeriodicSourceConnectorOptions Options { get; }

    public override async Task Execute(CancellationToken stoppingToken) {
        while (!stoppingToken.IsCancellationRequested) {
            try {
                await base.Execute(stoppingToken).ConfigureAwait(false);
                await Task.Delay(Options.BackoffTime, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) {
                // be kind and don't crash on cancellation
            }
        }
    }
}