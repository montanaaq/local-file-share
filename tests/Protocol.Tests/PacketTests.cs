namespace Protocol.Tests;

public class PacketTests
{
    public static IEnumerable<object[]> AllMessageTypes() =>
        Enum.GetValues<MessageType>().Select(t => new object[] { t });

    [Theory]
    [MemberData(nameof(AllMessageTypes))]
    public void Serialize_Parse_KeepsAllFields(MessageType messageType)
    {
        var originalPacket = new Packet(messageType, 42, [1, 2, 3]);
        var parsedPacket = Packet.Parse(originalPacket.Serialize());

        Assert.NotNull(parsedPacket);
        Assert.Equal(originalPacket.Type, parsedPacket.Type);
        Assert.Equal(originalPacket.Seq, parsedPacket.Seq);
        Assert.Equal(originalPacket.Payload, parsedPacket.Payload);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(uint.MaxValue)]
    public void Serialize_Parse_KeepsSeq_ForBoundaryValues(uint seq)
    {
        var originalPacket = new Packet(MessageType.Chunk, seq, [1, 2, 3]);
        var parsedPacket = Packet.Parse(originalPacket.Serialize());

        Assert.NotNull(parsedPacket);
        Assert.Equal(originalPacket.Type, parsedPacket.Type);
        Assert.Equal(originalPacket.Seq, parsedPacket.Seq);
        Assert.Equal(originalPacket.Payload, parsedPacket.Payload);
    }

    [Fact]
    public void Parse_ReturnsPacket_WhenPayloadIsEmpty()
    {
        var originalPacket = new Packet(MessageType.Heartbeat, 42, []);
        var parsedPacket = Packet.Parse(originalPacket.Serialize());

        Assert.NotNull(parsedPacket);
        Assert.Empty(parsedPacket.Payload);
        Assert.Equal(originalPacket.Type, parsedPacket.Type);
    }

    [Fact]
    public void Parse_ReturnsFullPayload_WhenPayloadIsMaxSize()
    {
        var longPayload = Enumerable.Range(0, Packet.MaxPayloadSize).Select(i => (byte)i).ToArray();
        var originalPacket = new Packet(MessageType.Chunk, 42, longPayload);
        var parsedPacket = Packet.Parse(originalPacket.Serialize());

        Assert.NotNull(parsedPacket);
        Assert.Equal(longPayload, parsedPacket.Payload);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void Parse_ReturnsNull_WhenBufferIsShorterThanHeader(int length)
    {
        var buffer = new byte[length];

        var parsedPacket = Packet.Parse(buffer);

        Assert.Null(parsedPacket);
    }

    [Fact]
    public void Serialize_ProducesExpectedBytes()
    {
        var packet = new Packet(MessageType.Chunk, 42, [1, 2, 3]);
        byte[] expected =
        [
            0x01,
            0x30, // version, type
            0x00,
            0x00,
            0x00,
            0x2A, // seq = 42
            0x00,
            0x03, // length = 3
            0xB9,
            0xF3,
            0x51,
            0x4E, // crc
            0x01,
            0x02,
            0x03, // payload
        ];
        Assert.Equal(expected, packet.Serialize());
    }
}
