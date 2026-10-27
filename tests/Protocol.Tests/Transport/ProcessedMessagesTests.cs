namespace Protocol.Tests;

public class ProcessedMessagesTests
{
    [Fact]
    public void Add_KeepsMessagesWithSameSeq_FromDifferentDevices()
    {
        var messages = new ProcessedMessages();
        var firstDeviceId = Guid.NewGuid();
        var secondDeviceId = Guid.NewGuid();
        const uint seq = 42;

        messages.Add(firstDeviceId, seq);

        Assert.False(messages.Contains(secondDeviceId, seq));
        Assert.False(messages.Contains(firstDeviceId, seq + 1));

        messages.Add(secondDeviceId, seq);

        Assert.True(messages.Contains(firstDeviceId, seq));
        Assert.True(messages.Contains(secondDeviceId, seq));
    }

    [Fact]
    public void Add_EvictsOldestMessage_OnlyWhenNewMessageExceedsCapacity()
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
