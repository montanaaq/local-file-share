namespace Protocol.Tests;

public class ProcessedMessagesTests
{
    [Fact]
    public void Add_MakesMessageAvailableToContains()
    {
        var messages = new ProcessedMessages();
        var deviceId = Guid.NewGuid();
        const uint seq = 42;

        Assert.False(messages.Contains(deviceId, seq));

        messages.Add(deviceId, seq);

        Assert.True(messages.Contains(deviceId, seq));
        Assert.False(messages.Contains(deviceId, seq + 1));
    }

    [Fact]
    public void Add_KeepsMessagesWithSameSeq_FromDifferentDevices()
    {
        var messages = new ProcessedMessages();
        var firstDeviceId = Guid.NewGuid();
        var secondDeviceId = Guid.NewGuid();
        const uint seq = 42;

        messages.Add(firstDeviceId, seq);

        Assert.False(messages.Contains(secondDeviceId, seq));

        messages.Add(secondDeviceId, seq);

        Assert.True(messages.Contains(firstDeviceId, seq));
        Assert.True(messages.Contains(secondDeviceId, seq));
    }

    [Fact]
    public void Add_RemovesOldestMessage_WhenCapacityIsExceeded()
    {
        var messages = new ProcessedMessages();
        var deviceId = Guid.NewGuid();
        uint capacity = (uint)ProtocolConstants.ProcessedMessagesCapacity;

        for (uint seq = 0; seq < capacity; seq++)
        {
            messages.Add(deviceId, seq);
        }

        Assert.True(messages.Contains(deviceId, 0));

        messages.Add(deviceId, capacity);

        Assert.False(messages.Contains(deviceId, 0));
        for (uint seq = 1; seq <= capacity; seq++)
        {
            Assert.True(messages.Contains(deviceId, seq));
        }
    }

    [Fact]
    public void Add_DoesNotEvictMessagesOrChangeOrder_WhenMessageAlreadyExists()
    {
        var messages = new ProcessedMessages();
        var deviceId = Guid.NewGuid();
        uint capacity = (uint)ProtocolConstants.ProcessedMessagesCapacity;

        for (uint seq = 0; seq < capacity; seq++)
        {
            messages.Add(deviceId, seq);
        }

        messages.Add(deviceId, 0);

        for (uint seq = 0; seq < capacity; seq++)
        {
            Assert.True(messages.Contains(deviceId, seq));
        }

        messages.Add(deviceId, capacity);

        Assert.False(messages.Contains(deviceId, 0));
        for (uint seq = 1; seq <= capacity; seq++)
        {
            Assert.True(messages.Contains(deviceId, seq));
        }
    }
}
