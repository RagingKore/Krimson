using System.Diagnostics;

namespace Krimson;

static class ObjectExtensions {
    [DebuggerStepThrough]
    public static T As<T>(this object source) => (T)source;
    
    [DebuggerStepThrough]
    public static T? MaybeAs<T>(this object? source) => (T?)source;
}