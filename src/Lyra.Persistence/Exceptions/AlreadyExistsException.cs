namespace Lyra.Persistence.Exceptions
{
    using System;

    public class AlreadyExistsException: Exception
    {
        public AlreadyExistsException()
            : this((string)null)
        {
        }

        public AlreadyExistsException(string message)
            : this(message, null)
        {
        }

        public AlreadyExistsException(Exception inner)
            : this(null, inner)
        {
        }

        public AlreadyExistsException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
