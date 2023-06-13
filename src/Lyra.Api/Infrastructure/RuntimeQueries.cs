namespace Lyra.Api.Infrastructure
{
    using System;

    public static class RuntimeQueries
    {
        public static readonly bool IsRunningAsLambda = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("LAMBDA_TASK_ROOT"));
    }
}
