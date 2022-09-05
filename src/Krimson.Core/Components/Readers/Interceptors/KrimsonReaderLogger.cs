using Krimson.Interceptors;

namespace Krimson.Readers.Interceptors;

[PublicAPI]
class KrimsonReaderLogger : InterceptorModule {
    public KrimsonReaderLogger() {
        On<RecordReceived>(
            evt => {
                Logger.Verbose(
                    "{ClientId} {Event} {RecordId} {MessageType}",
                    evt.Reader.ClientId, nameof(RecordReceived), evt.Record.ToString(), evt.Record.MessageType.Name
                );
            }
        );
    }
}