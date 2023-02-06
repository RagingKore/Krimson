namespace Krimson.Producers;

class InFlightMessageCounter {
    long _count;

    long Count => Interlocked.Read(ref _count);

    public long Increment() => Interlocked.Increment(ref _count);
    public long Decrement() => Interlocked.Decrement(ref _count);
    
    public static implicit operator long(InFlightMessageCounter self) => self.Count;
}