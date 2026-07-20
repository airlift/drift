namespace java io.airlift.drift.transport.netty.scribe.apache

enum ResultCode
{
  OK,
  TRY_LATER
}

struct LogEntry
{
  1:  string category,
  2:  string message
}

service scribe
{
  ResultCode Log(1: list<LogEntry> messages);
}
