using Confluent.Kafka;

namespace Krimson; 

public static class ConfluentClientExtensions {
    /// <summary>
    ///     Gets the name of this client instance.
    ///     Because it randomly crashes internally, seriously...
    /// 
    ///     Contains (but is not equal to) the client.id
    ///     configuration parameter.
    /// </summary>
    /// <remarks>
    ///     This name will be unique across all client
    ///     instances in a given application which allows
    ///     log messages to be associated with the
    ///     corresponding instance.
    /// </remarks>
    public static string GetInstanceName(this IClient client) {
        try {
            return client.Name;
        }
        catch (Exception) {
            return client.GetType().Name.Kebaberize(); //"unknown";
        }
    }
}