namespace Lyra.ManagementApi.Infrastructure
{
    using System;
    using Microsoft.Extensions.Configuration;

    public static class BuildInfoReader
    {
        public static (DateTime time, string commitHash) Read()
        {
            var model = new BuildInfo();

            var configRoot = new ConfigurationBuilder()
                .AddJsonFile("build.json", optional: true) //populated in build.csproj
                .Build();

            configRoot.Bind(model);

            return (time: model.BuildTime, commitHash: model.BuildCommitHash);
        }

        private class BuildInfo
        {
            public DateTime BuildTime { get; set; }
            public string BuildCommitHash { get; set; }
        }
    }
}
