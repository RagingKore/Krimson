// ReSharper disable PossibleMultipleEnumeration

namespace Krimson;

static class Ensure {
    public static T NotNull<T>(T argument, string argumentName) {
        if (argument is null)
            throw new ArgumentNullException(argumentName);

        return argument;
    }

    public static ReadOnlySpan<T> NotNullOrEmpty<T>(ReadOnlySpan<T> argument, string argumentName) {
        if (argument == null)
            throw new ArgumentNullException(argumentName);

        if (argument.IsEmpty)
            throw new ArgumentException($"{argumentName} must not be empty", argumentName);

        return argument;
    }

    public static ReadOnlyMemory<T> NotEmpty<T>(ReadOnlyMemory<T> argument, string argumentName) {
        if (argument.IsEmpty)
            throw new ArgumentException($"{argumentName} must not be empty", argumentName);

        return argument;
    }

    public static Span<T> NotNullOrEmpty<T>(Span<T> argument, string argumentName) {
        if (argument == null)
            throw new ArgumentNullException(argumentName);

        if (argument.IsEmpty)
            throw new ArgumentException($"{argumentName} must not be empty", argumentName);

        return argument;
    }

    public static Memory<T> NotEmpty<T>(Memory<T> argument, string argumentName) {
        if (argument.IsEmpty)
            throw new ArgumentException($"{argumentName} must not be empty", argumentName);

        return argument;
    }

    public static string NotNullOrEmpty(string? argument, string argumentName) {
        if (string.IsNullOrEmpty(argument))
            throw new ArgumentNullException(argumentName);

        return argument;
    }

    public static IEnumerable<T> NotNullOrEmpty<T>(IEnumerable<T> argument, string argumentName) {
        if (argument is null)
            throw new ArgumentNullException(argumentName);

        if (argument.Any() == false)
            throw new ArgumentException($"{argumentName} must not be empty", argumentName);

        return argument;
    }

    public static T[] NotNullOrEmpty<T>(T[] argument, string argumentName) {
        if (argument is null)
            throw new ArgumentNullException(argumentName);

        if (argument.Any() == false)
            throw new ArgumentException($"{argumentName} must not be empty", argumentName);

        return argument;
    }

    public static string NotNullOrWhiteSpace(string? argument, string argumentName) {
        if (string.IsNullOrWhiteSpace(argument))
            throw new ArgumentNullException(argument, argumentName);

        return argument;
    }

    public static char NotNull(char argument, string argumentName) {
        if (argument <= 0)
            throw new ArgumentOutOfRangeException(argumentName);

        return argument;
    }

    public static int Positive(int argument, string argumentName) {
        if (argument <= 0)
            throw new ArgumentOutOfRangeException(argumentName, $"{argumentName} must be positive.");

        return argument;
    }

    public static long Positive(long argument, string argumentName) {
        if (argument <= 0)
            throw new ArgumentOutOfRangeException(argumentName, $"{argumentName} must be positive.");

        return argument;
    }

    public static int Negative(int argument, string argumentName) {
        if (argument > 0)
            throw new ArgumentOutOfRangeException(argumentName, $"{argumentName} must be negative.");

        return argument;
    }

    public static long Negative(long argument, string argumentName) {
        if (argument > 0)
            throw new ArgumentOutOfRangeException(argumentName, $"{argumentName} must be negative.");

        return argument;
    }

    public static long NonNegative(long argument, string argumentName) {
        if (argument < 0)
            throw new ArgumentOutOfRangeException(argumentName, $"{argumentName} must be non-negative.");

        return argument;
    }

    public static int NonNegative(int argument, string argumentName) {
        if (argument < 0)
            throw new ArgumentOutOfRangeException(argumentName, $"{argumentName} must be non-negative.");

        return argument;
    }

    public static Guid NotEmptyGuid(Guid argument, string argumentName) {
        if (Guid.Empty == argument)
            throw new ArgumentException($"{argumentName} must not be empty.", argumentName);

        return argument;
    }

    public static Guid NotEmpty(Guid argument, string argumentName) {
        if (Guid.Empty == argument)
            throw new ArgumentException($"{argumentName} must not be empty.", argumentName);

        return argument;
    }

    public static T Valid<T>(T argument, string argumentName, Predicate<T> validator, string? errorMessage = null) {
        if (validator is null)
            throw new ArgumentNullException(nameof(validator), $"{argumentName} validator must not be null.");

        if (!validator(argument))
            throw new ArgumentException(argumentName, errorMessage ?? $"{argumentName} must be valid.");

        return argument;
    }
}