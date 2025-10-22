using NATS.Client.JetStream.Models;
using NATS.Client.JetStream;
using NATS.Net;

var nats = new NatsClient();
var jetStream = nats.CreateJetStreamContext();
try
{
    await jetStream.DeleteStreamAsync("ISTGITSATEST");
    Console.WriteLine("DELETED.");
}
catch
{
}

var stream = await jetStream.CreateOrUpdateStreamAsync(new(
    name: "ISTGITSATEST",
    subjects: [$"ISTGITSATEST.>"]
)
{
    MaxMsgsPerSubject = 1,
    Discard = StreamConfigDiscard.Old,
    DuplicateWindow = TimeSpan.Zero,
    Retention = StreamConfigRetention.Limits,
    AllowDirect = true,
});

foreach (var _ in Enumerable.Range(1, 1_000))
    await jetStream.PublishAsync($"ISTGITSATEST.{Guid.NewGuid()}", 123);
await Task.Delay(1_000);
while (true)
{
    var cts = new CancellationTokenSource();
    _ = Task.WhenAll(Enumerable.Range(1, 5).Select(i => SpawnConsumer(i, cts.Token)));
    Console.ReadLine();
    cts.Cancel();
    Console.WriteLine(new string('-', Console.WindowWidth));
}

async Task SpawnConsumer(int i, CancellationToken ct)
{
    // TestState.Current.Value = i;
    Console.WriteLine($"({i}) STARTED");
    try
    {
        var consumer = await stream.CreateOrUpdateConsumerAsync(new("foo"));
        while (!ct.IsCancellationRequested)
        {
            var enumerable = consumer.FetchNoWaitAsync<int>(new() { MaxMsgs = 1 }, cancellationToken: ct);
            var anything = false;
            await foreach (var item in enumerable)
            {
                Console.WriteLine($"({i}) RECEIVED: {item}");
                anything = true;
                await item.AckAsync();
            }
            Console.WriteLine($"({i}) LOOP EXITING (anything? {anything})");
        }
    }
    catch (Exception ex) when (ex is not OperationCanceledException)
    {
        Console.WriteLine(ex);
    }
}
