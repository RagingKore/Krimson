using JetBrains.Annotations;

namespace Krimson.Fixie;

[PublicAPI]
[AttributeUsage(AttributeTargets.Class)]
public class TestFixtureAttribute : Attribute { }

[PublicAPI]
[AttributeUsage(AttributeTargets.Method)]
public class TestAttribute : Attribute { }

[PublicAPI]
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
public class InlineDataAttribute : Attribute {
    public InlineDataAttribute(params object[] parameters) => Parameters = parameters;

    public object[] Parameters { get; }
}

[PublicAPI]
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
public class TestCaseAttribute : InlineDataAttribute {
    public TestCaseAttribute(params object[] parameters) : base(parameters) { }
}
