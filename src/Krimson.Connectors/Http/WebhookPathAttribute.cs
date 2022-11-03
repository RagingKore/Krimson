namespace Krimson.Connectors.Http;

[AttributeUsage(AttributeTargets.Class)]
public class WebhookPathAttribute : Attribute {
    public WebhookPathAttribute(string value) => Value = value;

    string Value { get; }

    public static implicit operator string(WebhookPathAttribute self) => self.Value;
    
    public static string GetValue(object source) => 
        (WebhookPathAttribute?)GetCustomAttribute(source.GetType(), typeof(WebhookPathAttribute)) ?? "";
}