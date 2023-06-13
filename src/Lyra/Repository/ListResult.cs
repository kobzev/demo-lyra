namespace Lyra.Repository
{
    using System.Collections.Generic;

    public class ListResult<T>
    {
        public List<T> Items { get; set; }

        public bool HasNext { get; set; } = false;

        public bool HasPrevious { get; set; } = false;

        public string Next { get; set; }

        public string Previous { get; set; }

        public int PageSize { get; set; } = 20;
    }
}
