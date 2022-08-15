namespace Krimson.Connectors.Http;

[AttributeUsage(AttributeTargets.Class)]
public class WebhookPathAttribute : Attribute {
    public WebhookPathAttribute(string value) => Value = value;

    string Value { get; }

    public static implicit operator string(WebhookPathAttribute self) => self.Value;
}