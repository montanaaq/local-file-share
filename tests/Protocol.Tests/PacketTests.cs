using Protocol.Messages;

namespace Protocol.Tests;

public class PacketTests
{
    [Theory]
    [InlineData(MessageType.Ack)]
    [InlineData(MessageType.Chunk)]
    public void Serialize_Parse_KeepsAllFields(MessageType messageType)
    {
        var originalPacket = new Packet(messageType, 42, [1, 2, 3]);
        var parsedPacket = Packet.Parse(originalPacket.Serialize());

        Assert.NotNull(parsedPacket);
        Assert.Equal(originalPacket.Type, parsedPacket.Type);
        Assert.Equal(originalPacket.Seq, parsedPacket.Seq);
        Assert.Equal(originalPacket.Payload, parsedPacket.Payload);
    }

    [Fact]
    public void Parse_ReturnsNull_WhenPayloadIsCorrupted()
    {
        byte[] bytes = new Packet(MessageType.Clipboard, 0, [1, 2, 3]).Serialize();
        bytes[^1] ^= 0xFF;
        Assert.Null(Packet.Parse(bytes));
    }

    [Fact]
    public void Parse_ReturnsNull_WhenLengthDoesNotMatch()
    {
        byte[] bytes = new Packet(MessageType.Clipboard, 0, [1, 2, 3]).Serialize();
        Assert.Null(Packet.Parse(bytes.AsSpan(0, bytes.Length - 1)));
        Assert.Null(Packet.Parse([.. bytes, 0]));
    }
}
