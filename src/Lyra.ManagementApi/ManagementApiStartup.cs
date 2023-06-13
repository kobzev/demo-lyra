namespace Lyra.ManagementApi
{
    using System;
    using System.IdentityModel.Tokens.Jwt;
    using System.Net.Http;
    using Amazon.DynamoDBv2;
    using Amazon.XRay.Recorder.Core.Strategies;
    using Identifi;
    using LykkeCorp.Bill.LogEnrichment;
    using Lyra.Api.Infrastructure;
    using Lyra.ManagementApi.Controllers;
    using Lyra.ManagementApi.Models;
    using Lyra.Persistence;
    using Lyra.Repository;
    using Microsoft.AspNetCore.Authentication.JwtBearer;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    public class ManagementApiStartup
    {
        private readonly Func<HttpMessageHandler> getBackChannelHandler;
        private readonly ManagementApiConfiguration managementApiConfiguration;
        private readonly IConfiguration appConfiguration;

        public ManagementApiStartup(
            IConfiguration appConfiguration,
            Func<HttpMessageHandler> getBackChannelHandler)
        {
            this.getBackChannelHandler = getBackChannelHandler;
            this.managementApiConfiguration = new ManagementApiConfiguration();
            appConfiguration.Bind(this.managementApiConfiguration);
            this.appConfiguration = appConfiguration;
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton(this.managementApiConfiguration);
            
            JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear();

            services.AddEnrichedLogging();
            services.AddIdentifi().InAspNetCoreMode();

            services.AddAuthentication(options =>
            {
                options.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                options.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            }).AddJwtBearer(options =>
            {
                options.BackchannelHttpHandler = this.getBackChannelHandler();
                options.RequireHttpsMetadata = !this.managementApiConfiguration.Auth.InternalStsUrl.StartsWith("http://");
                options.Authority = this.managementApiConfiguration.Auth.InternalStsUrl;
                options.Audience = this.managementApiConfiguration.Auth.ClientId;
                options.TokenValidationParameters.RoleClaimType = "role";
            });

            services
                .AddMvc(options =>
                {
                    options.EnableEndpointRouting = false;
                })
                .AddNewtonsoftJson(jsonOptions =>
                {
                    jsonOptions.SerializerSettings.DateFormatHandling = DateFormatHandling.IsoDateFormat;
                    jsonOptions.SerializerSettings.ContractResolver = new CamelCasePropertyNamesContractResolver();
                    jsonOptions.SerializerSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter { NamingStrategy = new CamelCaseNamingStrategy() });
                })
                .UseSpecificControllers(
                    typeof(ProductsController),
                    typeof(InstrumentsController),
                    typeof(CryptoController),
                    typeof(ShareTokensController),
                    typeof(SimpleController),
                    typeof(FiatController)
                    );

            services.AddAuthorization(options =>
            {
                options.AddPolicy(Infrastructure.Constants.SecurityPolicy, policy =>
                {
                    policy.RequireAuthenticatedUser();
                });
            });

            services.AddIdentifi().InAspNetCoreMode();
            services.AddHttpClient().AddIdentifiHttpClient();

            services.AddSingleton(this.managementApiConfiguration.Auth);

            services.AddSingleton<ProductsResponseFactory>();
            
            services.TryAddSingleton<IAmazonDynamoDB, AmazonDynamoDBClient>();
            services.TryAddSingleton<IProductWriteRepository, ProductWriteRepository>();
            services.TryAddSingleton<IProductReadRepository, ProductReadRepository>();
        }

        public void Configure(IApplicationBuilder application, IWebHostEnvironment environment)
        {
            if (RuntimeQueries.IsRunningAsLambda)
            {
                // XRAY is configured here
                application.UseXRay(new DynamicSegmentNamingStrategy("Lyra.ManagementApi"), this.appConfiguration);
            }

            application.Use(async (context, next) =>
            {
                var logger = application.ApplicationServices.GetService<ILogger<ManagementApiStartup>>();
                var path = context.Request.Path;
                logger?.LogCritical(path);
                await next.Invoke();
            });

            
            application.UseIdentifi();
            application.UseEnrichedLogging();
            if (environment.IsDevelopment())
            {
                application.UseDeveloperExceptionPage();
            }
            else
            {
                application.UseHsts();
            }
            application.UseAuthentication();
            application.UseMvc();
        }
    }
}
