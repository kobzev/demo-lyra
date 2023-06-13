namespace Lyra.Api.Infrastructure
{
    using System;
    using System.Net.Http;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    public static class HttpClientExtensions
    {
        public static async Task<(HttpResponseMessage HttpResponse, T Data)> GetAs<T>(this HttpClient client, string url, CancellationToken cancellationToken = default)
        {
            var response = await client.GetAsync(url, cancellationToken);
            var content = await response.ReadContent();
            var deserialize = response.IsSuccessStatusCode && !string.IsNullOrWhiteSpace(content);
            var data = deserialize ? JsonConvert.DeserializeObject<T>(content) : default;
            return (response, data);
        }

        public static async Task<(HttpResponseMessage HttpResponse, T Data)> PostAsJsonAndReadAs<T>(this HttpClient client, string url, object model, CancellationToken cancellationToken = default)
        {
            var response = await client.PostAsync(url, new StringContent(JsonConvert.SerializeObject(model), Encoding.UTF8, "application/json"), cancellationToken);
            var content = await response.ReadContent();
            var deserialize = response.IsSuccessStatusCode && !string.IsNullOrWhiteSpace(content);
            var data = deserialize ? JsonConvert.DeserializeObject<T>(content) : default;
            return (response, data);
        }

        public static async Task<(HttpResponseMessage HttpResponse, T Data)> PostAndReadAs<T>(this HttpClient client, string url, HttpContent httpContent, CancellationToken cancellationToken = default)
        {
            var response = await client.PostAsync(url, httpContent, cancellationToken);
            var content = await response.ReadContent();
            var deserialize = response.IsSuccessStatusCode && !string.IsNullOrWhiteSpace(content);
            var data = deserialize ? JsonConvert.DeserializeObject<T>(content) : default;
            return (response, data);
        }

        public static async Task<HttpResponseMessage> PostAsJson(this HttpClient client, string url, object model, CancellationToken cancellationToken = default)
        {
            return await client.PostAsync(url, new StringContent(JsonConvert.SerializeObject(model), Encoding.UTF8, "application/json"), cancellationToken);
        }

        public static async Task<HttpResponseMessage> PutAsJson(this HttpClient client, string url, object model, CancellationToken cancellationToken = default)
        {
            return await client.PutAsync(url, new StringContent(JsonConvert.SerializeObject(model), Encoding.UTF8, "application/json"), cancellationToken);
        }

        public static async Task<string> ReadContent(this HttpResponseMessage message)
        {
            if (message?.Content == null)
            {
                return string.Empty;
            }

            try
            {
                return await message.Content.ReadAsStringAsync();
            }
            catch (Exception)
            {
                // there are situations where the content is not available. Don't throw as this method is typically used
                // during error handling. 
                return "";
            }
        }
    }
}
