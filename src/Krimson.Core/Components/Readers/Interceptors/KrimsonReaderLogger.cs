using Krimson.Interceptors;

namespace Krimson.Readers.Interceptors;

[PublicAPI]
class KrimsonReaderLogger : InterceptorModule {
    public KrimsonReaderLogger() {
        On<RecordReceived>(
            evt => {
             
                 Logger
                     .Verbose(
                        "{Event} {RecordId} {MessageType}",
                        nameof(RecordReceived), evt.Record, evt.Record.MessageType.Name
                    );
            }
        );
    }
}