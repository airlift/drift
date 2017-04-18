# Swift

Swift is an easy-to-use, annotation-based Java library for creating Thrift
serializable types and services.

# Swift Codec

[Swift Codec](swift-codec) is a simple library specifying how Java
objects are converted to and from Thrift.  This library is similar to JaxB
(XML) and Jackson (JSON), but for Thrift.  Swift codec supports field, method,
constructor, and builder injection.  For example:

    @ThriftStruct
    public class LogEntry
    {
        private final String category;
        private final String message;

        @ThriftConstructor
        public LogEntry(String category, String message)
        {
            this.category = category;
            this.message = message;
        }

        @ThriftField(1)
        public String getCategory()
        {
            return category;
        }

        @ThriftField(2)
        public String getMessage()
        {
            return message;
        }
    }
