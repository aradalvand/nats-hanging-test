using NATS.Client.JetStream.Models;
using NATS.Client.JetStream;
using NATS.Net;

var nats = new NatsClient();
var jetStream = nats.CreateJetStreamContext();
try
{
    await jetStream.DeleteStreamAsync("NATS_HANGING_ISSUE");
    Console.WriteLine("DELETED.");
}
catch
{
}

var stream = await jetStream.CreateOrUpdateStreamAsync(new(
    name: "NATS_HANGING_ISSUE",
    subjects: [$"NATS_HANGING_ISSUE.>"]
)
{
    MaxMsgsPerSubject = 1,
    Discard = StreamConfigDiscard.Old,
    DuplicateWindow = TimeSpan.Zero,
    Retention = StreamConfigRetention.Limits,
    AllowDirect = true,
});

foreach (var _ in Enumerable.Range(1, 10_000))
    await jetStream.PublishAsync($"NATS_HANGING_ISSUE.{Guid.NewGuid()}", 123);

var consumers = Enumerable.Range(1, 5).Select(SpawnConsumer);
await Task.WhenAll(consumers);

async Task SpawnConsumer(int i)
{
    Console.WriteLine($"({i}) STARTED");
    try
    {
        var consumer = await stream.CreateOrUpdateConsumerAsync(new("foo"));
        while (true)
        {
            // NOTE: If `1` is changed to any greater number (including `2`), the problem will go away.
            var anything = false;
            await foreach (var item in consumer.FetchNoWaitAsync<int>(new() { MaxMsgs = 1 }))
            {
                Console.WriteLine($"({i}) RECEIVED `{item.Subject}`");
                anything = true;
                await item.AckAsync();
            }
            if (!anything)
            {
                Console.WriteLine($"({i}) DONE.");
                return;
            }
            Console.WriteLine($"({i}) LOOP EXITING (anything? {anything})");
        }
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
        Console.WriteLine(ex);
    }
}
