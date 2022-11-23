// using Krimson.Examples.Messages.Telemetry;
// using Krimson.Producers;
// using Microsoft.AspNetCore.Mvc;
//
// namespace Telemetry.Gateway.Http.Controllers;
//
// [ApiController]
// [Route("[controller]")]
// public class IngressController : ControllerBase {
//     public IngressController(KrimsonProducer producer) => Producer = producer;
//
//     KrimsonProducer Producer { get; }
//     
//     [HttpPost(Name = "PowerConsumption")]
//     public async Task Set(PowerConsumption telemetry) {
//         await Producer.Produce(
//             message: telemetry,
//             key: telemetry.DeviceId
//         );
//     }
// }