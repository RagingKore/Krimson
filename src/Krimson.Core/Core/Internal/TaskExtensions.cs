namespace Krimson;

static class TaskExtensions {
    public static void Synchronously(this Task task) => task.GetAwaiter().GetResult();

    public static T Synchronously<T>(this Task<T> task) => task.GetAwaiter().GetResult();
}