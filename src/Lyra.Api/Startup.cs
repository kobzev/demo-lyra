namespace Lyra.Api
{
    using Amazon.DynamoDBv2;
    using Amazon.XRay.Recorder.Core;
    using Amazon.XRay.Recorder.Core.Strategies;
    using Identifi;
    using LykkeCorp.Bill.LogEnrichment;
    using Lyra.Api.Configuration;
    using Lyra.Api.Controllers;
    using Lyra.Api.Infrastructure;
    using Lyra.Persistence;
    using Lyra.Products;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Authentication.JwtBearer;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Hosting;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using System;
    using System.Net.Http;

    public class Startup
    {
        private readonly LyraConfiguration config;
        private readonly IConfiguration appConfiguration;
        private readonly Func<HttpMessageHandler> getBackChannelHandler;

        public Startup(IConfiguration configuration, Func<HttpMessageHandler> getBackChannelHandler)
        {
            this.getBackChannelHandler = getBackChannelHandler;
            this.config = new LyraConfiguration();
            configuration.Bind(this.config);
            
            this.appConfiguration = configuration;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services
                .AddAuthentication(options =>
                {
                    options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                    options.DefaultScheme = JwtBearerDefaults.AuthenticationScheme;
                    options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
                })
                .AddJwtBearer(options =>
                {
                    options.Authority = this.config.Auth.InternalStsUrl;
                    options.Audience = this.config.Auth.ClientId;
                    options.BackchannelHttpHandler = this.getBackChannelHandler();
                    options.RequireHttpsMetadata = !this.config.Auth.InternalStsUrl.StartsWith("http://");
                });

            services
                .AddMvc(options => options.EnableEndpointRouting = false)
                .AddNewtonsoftJson(jsonOptions =>
                {
                    jsonOptions.SerializerSettings.DateFormatHandling = DateFormatHandling.IsoDateFormat;
                    jsonOptions.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                    jsonOptions.SerializerSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter { CamelCaseText = true });
                })
                .UseSpecificControllers(
                    typeof(ProductsController),
                    typeof(InstrumentsController));

            // Optional services, to be overwritten in unit tests. 


            // Required services
            services.AddIdentifiHttpClient();

            services.AddSingleton(this.config);

            // Optional services, to be overwritten in unit tests. 
            services.TryAddSingleton<IAmazonDynamoDB, AmazonDynamoDBClient>();
            services.TryAddSingleton<IProductWriteRepository, ProductWriteRepository>();
            services.TryAddSingleton<IProductReadRepository, ProductReadRepository>();
            
            services.AddSingleton<IAWSXRayRecorder>(AWSXRayRecorder.Instance);

            services.AddIdentifi().InAspNetCoreMode();
            services.AddEnrichedLogging();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseIdentifi();
            app.UseEnrichedLogging();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseAuthentication();

            app.UseMvc();

            if (RuntimeQueries.IsRunningAsLambda)
            {
                app.UseXRay(new DynamicSegmentNamingStrategy("Lyra.Api"), this.appConfiguration);
            }
        }
    }
}
