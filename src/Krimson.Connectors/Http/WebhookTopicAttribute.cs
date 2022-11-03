// namespace Krimson.Connectors.Http;
//
// [AttributeUsage(AttributeTargets.Class)]
// public class WebhookTopicAttribute : Attribute {
//     public WebhookTopicAttribute(string value) => Value = value;
//
//     string Value { get; }
//     
//     public static implicit operator string(WebhookTopicAttribute self) => self.Value;
//
//     public static string GetValue(object source) {
//         var att = (WebhookTopicAttribute)GetCustomAttribute(source.GetType(), typeof(WebhookTopicAttribute))!;
//         return att.Value;
//     }
// }