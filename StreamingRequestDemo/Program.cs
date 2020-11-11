using System;
using System.Collections;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace StreamingRequestDemo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //{
            //    var content = true;
            //    //var content = 123.456;
            //    //var content = "pippo";
            //    //var content = new DateTime(1966, 7, 23);
            //    string s = JsonSerializer.Serialize(content);
            //    Console.WriteLine(s);
            //    var d = JsonSerializer.Deserialize<bool>(s);
            //    Console.WriteLine(d);
            //}

            var sw = Stopwatch.StartNew();
            var ta = new Task[]
            {
                //Demo1Async(),
                //Demo1Async(),
                //Demo1Async(),

                Demo2Async(),
            };
            await Task.WhenAll(ta);

            //await Demo1Async();
            sw.Stop();
            Console.WriteLine("time=" + sw.ElapsedMilliseconds);
        }


        static async Task Demo1Async()
        {
            int count = 0;
            long totalBytesRead = await Downloader(
                new Uri("https://localhost:5001/longrunning/demo1"),
                s =>
                {
                    double[] riga = JsonSerializer.Deserialize<double[]>(s);
                    count++;
                    if ((count % 1000) == 0)
                    {
                        Console.WriteLine(count);
                    }
                });
            Console.WriteLine($"totali={totalBytesRead}");
        }


        static async Task<long> Downloader(
            Uri uri,
            Action<string> callback
            )
        {
            using var client = new HttpClient();

            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("text/plain")
                );
            client.DefaultRequestHeaders.Add("User-Agent", ".NET Foundation Repository Reporter");

            var stream = await client.GetStreamAsync(uri);

            var buffer = new byte[0x10000];
            int bytesInBuffer = 0;
            long totalBytesRead = 0;
            while (true)
            {
                //accumula i bytes in arrivo sul buffer
                bool isFinalBlock = false;
                while (true)
                {
                    int bytesRead = await stream
                        .ReadAsync(buffer, bytesInBuffer, buffer.Length - bytesInBuffer)
                        .ConfigureAwait(false);

                    if (bytesRead == 0)
                    {
                        isFinalBlock = true;
                        break;
                    }

                    totalBytesRead += bytesRead;
                    bytesInBuffer += bytesRead;
                    if (bytesInBuffer == buffer.Length)
                    {
                        break;
                    }
                }

                //isola i vari segmenti in base al carattere CR
                int lastIx = 0;
                for (int ix = 0; ix < bytesInBuffer; ix++)
                {
                    if (buffer[ix] == '\r')
                    {
                        string s = Encoding.UTF8.GetString(buffer, lastIx, ix - lastIx);
                        lastIx = ix + 1;
                        callback(s);
                    }
                }

                if (isFinalBlock)
                {
                    break;
                }

                //aggiusta il buffer
                if (lastIx < buffer.Length)
                {
                    Buffer.BlockCopy(buffer, lastIx, buffer, 0, buffer.Length - lastIx);
                }
                bytesInBuffer -= lastIx;

            }
            return totalBytesRead;
        }


        static async Task Demo2Async()
        {
            int count = 0;
            long totalBytesRead = await Downloader2(
                new Uri("https://localhost:5001/longrunning/demo2"),
                cb =>
                {
                    switch (cb.FieldName)
                    {
                        case "nome":
                            Console.WriteLine($"nome={cb.GetValue<string>()}");
                            break;

                        case "nato":
                            Console.WriteLine($"nato={cb.GetValue<DateTime>()}");
                            break;

                        case "count":
                            Console.WriteLine($"count={cb.GetValue<int>()}");
                            break;

                        case "matrice":
                            double[] riga = cb.GetValue<double[]>();
                            break;
                    }

                    count++;
                    if ((count % 1000) == 0)
                    {
                        Console.WriteLine(count);
                    }
                });
            Console.WriteLine($"totali={totalBytesRead}");
        }


        interface IReadOnlyCallback
        {
            string FieldName { get; }
            int Index { get; }
            TValue GetValue<TValue>();
        }

        class Callback : IReadOnlyCallback
        {
            public string FieldName { get; set; }
            public int Index { get; set; }

            public string _json;
            public TValue GetValue<TValue>() => JsonSerializer.Deserialize<TValue>(this._json);
        }

        static async Task<long> Downloader2(
            Uri uri,
            Action<IReadOnlyCallback> callback
            )
        {
            using var client = new HttpClient();

            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(
                new MediaTypeWithQualityHeaderValue("text/plain")
                );
            client.DefaultRequestHeaders.Add("User-Agent", ".NET Foundation Repository Reporter");

            var stream = await client.GetStreamAsync(uri);

            var buffer = new byte[0x10000];
            int bytesInBuffer = 0;
            long totalBytesRead = 0;
            var cb = new Callback();
            while (true)
            {
                //accumula i bytes in arrivo sul buffer
                bool isFinalBlock = false;
                while (true)
                {
                    int bytesRead = await stream
                        .ReadAsync(buffer, bytesInBuffer, buffer.Length - bytesInBuffer)
                        .ConfigureAwait(false);

                    if (bytesRead == 0)
                    {
                        isFinalBlock = true;
                        break;
                    }

                    totalBytesRead += bytesRead;
                    bytesInBuffer += bytesRead;
                    if (bytesInBuffer == buffer.Length)
                    {
                        break;
                    }
                }

                //isola i vari segmenti in base al carattere CR
                int lastIx = 0;
                for (int ix = 0; ix < bytesInBuffer; ix++)
                {
                    if (buffer[ix] == '\r')
                    {
                        if (buffer[lastIx] == '\a')
                        {
                            cb.FieldName = Encoding.UTF8.GetString(buffer, lastIx + 1, ix - lastIx - 1);
                            cb.Index = -1;
                        }
                        else
                        {
                            cb._json = Encoding.UTF8.GetString(buffer, lastIx, ix - lastIx);
                            cb.Index++;
                            callback(cb);
                        }
                        lastIx = ix + 1;
                    }
                }

                if (isFinalBlock)
                {
                    break;
                }

                //aggiusta il buffer
                if (lastIx < buffer.Length)
                {
                    Buffer.BlockCopy(buffer, lastIx, buffer, 0, buffer.Length - lastIx);
                }
                bytesInBuffer -= lastIx;

            }
            return totalBytesRead;
        }

    }
}
