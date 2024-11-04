using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Npgsql;

async IAsyncEnumerable<string> listenNotifyItem(string connectionString, string channelName,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    var channel = Channel.CreateBounded<string>(new BoundedChannelOptions(10)
    {
        SingleReader = true,
        SingleWriter = true,
        FullMode = BoundedChannelFullMode.Wait,
    });

    Task SubscribeAsync(CancellationToken token)
    {
        return Task.Run(async () =>
        {
            var connection = new NpgsqlConnection(connectionString);
            try
            {
                Console.WriteLine("open connection");
                await connection.OpenAsync(cancellationToken);

                Console.WriteLine("open command");
                await using var command = connection.CreateCommand();
                command.CommandText = $"LISTEN {channelName};";
                connection.Notification += (o, e) => channel.Writer.TryWrite(e.Payload);
                await command.ExecuteNonQueryAsync(token);

                while (!token.IsCancellationRequested)
                {
                    Console.WriteLine($"Execute and wait notification {connection.FullState}");
                    await command.ExecuteNonQueryAsync(token);
                    await connection.WaitAsync(TimeSpan.FromSeconds(30), token);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Got error ${e}");
                Console.WriteLine("Close connection");
                await Task.Delay(TimeSpan.FromSeconds(10));
                _ = SubscribeAsync(token);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }, token).ContinueWith(t =>
        {
            Console.WriteLine($"Task {t.Id} is raised {t.Status}, with Exception {t.Exception}");
        });
    }

    _ = SubscribeAsync(cancellationToken);

    var asyncEnumerable = channel.Reader.ReadAllAsync(cancellationToken);
    await foreach (var item in asyncEnumerable)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            yield break;
        }

        yield return item;
    }

    Console.WriteLine("Stop listening channel");
}

var connStr = Environment.GetEnvironmentVariable("CONNECTION_STRING") ??
              "Host=localhost;Username=postgres;Password=postgres;Database=postgres";
var channelStr = Environment.GetEnvironmentVariable("CHANNEL_NAME") ?? "notify";
await foreach (var s in listenNotifyItem(
                   connStr,
                   channelStr, default))
{
    Console.WriteLine($"Received notification: {s}");
}

Console.WriteLine("Stop listening");