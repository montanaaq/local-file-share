using System;
using System.Collections.Generic;

namespace Protocol;

public sealed class ProcessedMessages
{
    private readonly HashSet<(Guid DeviceId, uint Seq)> _messages = [];
    private readonly Queue<(Guid DeviceId, uint Seq)> _order = new();

    public bool Contains(Guid deviceId, uint seq)
    {
        return _messages.Contains((deviceId, seq));
    }

    public void Add(Guid deviceId, uint seq)
    {
        var key = (deviceId, seq);
        if (!_messages.Add(key))
        {
            return;
        }

        _order.Enqueue(key);
        if (_order.Count > ProtocolConstants.ProcessedMessagesCapacity)
        {
            var oldest = _order.Dequeue();
            _messages.Remove(oldest);
        }
    }
}
