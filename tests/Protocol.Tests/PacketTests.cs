using System.Buffers.Binary;
using System.IO.Hashing;

namespace Protocol.Tests;

public class PacketTests
{
    [Theory]
    [InlineData(0x01)]
    [InlineData(0x04)]
    [InlineData(0x10)]
    [InlineData(0xFF)]
    public void Packet_RejectsRemovedAndUnknownMessageTypes(byte type)
    {
        Assert.Throws<ArgumentException>(() => new Packet((MessageType)type, 0, []).Serialize());

        byte[] bytes = new Packet(MessageType.Ack, 0, []).Serialize();
        bytes[1] = type;
        bytes.AsSpan(8, 4).Clear();
        BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(8), Crc32.HashToUInt32(bytes));
        Assert.Null(Packet.Parse(bytes));
    }

    [Fact]
    public void Serialize_Throws_WhenPayloadExceedsLimit()
    {
        Assert.Throws<ArgumentException>(() =>
            new Packet(MessageType.Chunk, 0, new byte[1061]).Serialize()
        );
    }

    [Fact]
    public void Parse_ReturnsNull_WhenPayloadExceedsLimitWithValidCrc()
    {
        byte[] bytes = new byte[12 + 1061];
        bytes[0] = 1;
        bytes[1] = (byte)MessageType.Chunk;
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(6), 1061);
        BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(8), Crc32.HashToUInt32(bytes));

        Assert.Null(Packet.Parse(bytes));
    }

    [Fact]
    public void Parse_ReturnsNull_WhenPayloadIsCorrupted()
    {
        byte[] bytes = new Packet(MessageType.Clipboard, 0, [1, 2, 3]).Serialize();
        bytes[^1] ^= 0xFF;
        Assert.Null(Packet.Parse(bytes));
    }

    [Fact]
    public void Parse_ReturnsNull_WhenVersionIsInvalidWithValidCrc()
    {
        byte[] bytes = new Packet(MessageType.Ack, 0, []).Serialize();
        bytes[0] = 2;
        bytes.AsSpan(8, 4).Clear();
        BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(8), Crc32.HashToUInt32(bytes));
        Assert.Null(Packet.Parse(bytes));
    }

    [Fact]
    public void Parse_ReturnsNull_WhenLengthDoesNotMatch()
    {
        byte[] bytes = new Packet(MessageType.Clipboard, 0, [1, 2, 3]).Serialize();
        Assert.Null(Packet.Parse(bytes.AsSpan(0, bytes.Length - 1)));
        Assert.Null(Packet.Parse([.. bytes, 0]));
    }

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
    public void Parse_ReturnsPacket_WhenHeartbeatContainsDeviceId()
    {
        var originalPacket = new Packet(
            MessageType.Heartbeat,
            42,
            Guid.NewGuid().ToByteArray(true)
        );
        var parsedPacket = Packet.Parse(originalPacket.Serialize());

        Assert.NotNull(parsedPacket);
        Assert.Equal(originalPacket.Payload, parsedPacket.Payload);
        Assert.Equal(originalPacket.Type, parsedPacket.Type);
    }

    [Fact]
    public void Parse_ReturnsFullPayload_WhenPayloadIsMaxSize()
    {
        byte[] longPayload = Enumerable
            .Range(0, Packet.MaxPayloadSize)
            .Select(i => (byte)i)
            .ToArray();
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
        byte[] buffer = new byte[length];

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
            0x30, // Version and type
            0x00,
            0x00,
            0x00,
            0x2A, // Sequence number: 42
            0x00,
            0x03, // Length: 3
            0xB9,
            0xF3,
            0x51,
            0x4E, // Checksum
            0x01,
            0x02,
            0x03, // Payload
        ];
        Assert.Equal(expected, packet.Serialize());
    }
}
