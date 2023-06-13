namespace Lyra.Api
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Lyra.Api.Models.Products;
    using Lyra.Products;

    public static class Helpers
    {
        public static IEnumerable<ProductTypeModel> FilterFromUserInput(string userInput, string[] allowedInstruments = null)
        {
            if (allowedInstruments == null)
            {
                allowedInstruments = ProductTypes.Products;
            }

            return userInput.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Trim().ToLowerInvariant())
                    .Where(x => allowedInstruments.Contains(x, StringComparer.OrdinalIgnoreCase)).ToArray()
                    .Select(x =>
                        Enum.TryParse<ProductTypeModel>(x, true, out var type) ?
                            type :
                            ProductTypeModel.Unknown)
                    .Where(x => x != ProductTypeModel.Unknown);
        }
    }
}
