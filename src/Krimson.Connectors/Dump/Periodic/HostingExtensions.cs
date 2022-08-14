// namespace Krimson.Connectors;
//
// [PublicAPI]
// public static class ServicesExtensions {
//     public static IServiceCollection AddKrimsonPeriodicSource<T>(this IServiceCollection services) where T : PeriodicSourceModule =>
//         services
//             .AddSingleton<T>()
//             .AddHostedService<PeriodicSourceService<T>>();
// }