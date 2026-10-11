using System.Buffers.Binary;

namespace Protocol.Tests;

public class PayloadsTests
{
    [Fact]
    public void EncodeChunk_ProducesExpectedBigEndianBytes()
    {
        var encoded = Payloads.EncodeChunk(
            Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"),
            Guid.Parse("ffeeddcc-bbaa-9988-7766-554433221100"),
            0x01020304,
            [0xAA, 0xBB]
        );

        var expected = Convert.FromHexString(
            "00112233445566778899AABBCCDDEEFF" + "FFEEDDCCBBAA99887766554433221100" + "01020304AABB"
        );
        Assert.Equal(expected, encoded);
        var decoded = Payloads.DecodeChunk(expected);
        Assert.NotNull(decoded);
        Assert.Equal(Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), decoded.Value.DeviceId);
        Assert.Equal(Guid.Parse("ffeeddcc-bbaa-9988-7766-554433221100"), decoded.Value.FileId);
        Assert.Equal(0x01020304u, decoded.Value.Index);
        Assert.Equal(new byte[] { 0xAA, 0xBB }, decoded.Value.Data);
    }

    [Fact]
    public void EncodeAck_ProducesExpectedBigEndianBytes()
    {
        var deviceId = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        var expected = Convert.FromHexString("00112233445566778899AABBCCDDEEFF01020304");

        Assert.Equal(expected, Payloads.EncodeAck(deviceId, 0x01020304));
        var decoded = Payloads.DecodeAck(expected);
        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(0x01020304u, decoded.Value.AckedSeq);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(19)]
    [InlineData(21)]
    public void DecodeAck_ReturnsNull_WhenLengthIsInvalid(int length)
    {
        Assert.Null(Payloads.DecodeAck(new byte[length]));
    }

    [Fact]
    public void EncodeChunk_Throws_WhenDataExceedsLimit()
    {
        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, new byte[1025])
        );
        Assert.Null(Payloads.DecodeChunk(new byte[36 + 1025]));
    }

    [Fact]
    public void EncodeChunk_FitsMaximumPacket_WhenDataIs1024Bytes()
    {
        var encoded = Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, new byte[1024]);
        var decoded = Payloads.DecodeChunk(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(1024, decoded.Value.Data.Length);
        Assert.Equal(1060, encoded.Length);
        Assert.Equal(1072, new Packet(MessageType.Chunk, 0, encoded).Serialize().Length);
    }

    [Fact]
    public void EncodeFileOffer_ProducesExpectedBigEndianBytes()
    {
        var expected = Convert.FromHexString(
            "00112233445566778899AABBCCDDEEFF"
                + "FFEEDDCCBBAA99887766554433221100"
                + "0000000000010203"
                + new string('0', 64)
                + "05612E747874"
        );
        var encoded = Payloads.EncodeFileOffer(
            Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"),
            Guid.Parse("ffeeddcc-bbaa-9988-7766-554433221100"),
            0x010203,
            new byte[32],
            "a.txt"
        );

        Assert.Equal(expected, encoded);
        var decoded = Payloads.DecodeFileOffer(expected);
        Assert.NotNull(decoded);
        Assert.Equal(Guid.Parse("00112233-4455-6677-8899-aabbccddeeff"), decoded.Value.DeviceId);
        Assert.Equal(Guid.Parse("ffeeddcc-bbaa-9988-7766-554433221100"), decoded.Value.FileId);
        Assert.Equal(0x010203ul, decoded.Value.Size);
        Assert.Equal("a.txt", decoded.Value.Name);
    }

    [Theory]
    [InlineData(0ul)]
    [InlineData(20971520ul)]
    public void EncodeDecodeFileOffer_KeepsBoundarySizeAndUtf8Name(ulong size)
    {
        var encoded = Payloads.EncodeFileOffer(
            Guid.NewGuid(),
            Guid.NewGuid(),
            size,
            new byte[32],
            "фото.jpg"
        );
        var decoded = Payloads.DecodeFileOffer(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(size, decoded.Value.Size);
        Assert.Equal("фото.jpg", decoded.Value.Name);
    }

    [Fact]
    public void FileOffer_RejectsSizeAboveLimit()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Payloads.EncodeFileOffer(
                Guid.NewGuid(),
                Guid.NewGuid(),
                20971521,
                new byte[32],
                "a.txt"
            )
        );
        var encoded = Payloads.EncodeFileOffer(
            Guid.NewGuid(),
            Guid.NewGuid(),
            1,
            new byte[32],
            "a.txt"
        );
        BinaryPrimitives.WriteUInt64BigEndian(
            encoded.AsSpan(PayloadsConstants.FileSizeOffset),
            20971521
        );
        Assert.Null(Payloads.DecodeFileOffer(encoded));
    }

    [Fact]
    public void FileOffer_RejectsEmptyName()
    {
        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeFileOffer(Guid.NewGuid(), Guid.NewGuid(), 1, new byte[32], "")
        );
        Assert.Null(Payloads.DecodeFileOffer(new byte[PayloadsConstants.NameOffset]));
    }

    [Fact]
    public void DecodeFileOffer_ReturnsNull_WhenNameIsInvalidUtf8()
    {
        var encoded = Payloads.EncodeFileOffer(
            Guid.NewGuid(),
            Guid.NewGuid(),
            1,
            new byte[32],
            "a"
        );
        encoded[PayloadsConstants.NameOffset] = 0xFF;
        Assert.Null(Payloads.DecodeFileOffer(encoded));
    }

    [Fact]
    public void DecodeFileOffer_ReturnsNull_WhenBufferIsTruncated()
    {
        var encoded = Payloads.EncodeFileOffer(
            Guid.NewGuid(),
            Guid.NewGuid(),
            1,
            new byte[32],
            "a.txt"
        );
        Assert.Null(Payloads.DecodeFileOffer(encoded.AsSpan(0, PayloadsConstants.NameOffset - 1)));
        Assert.Null(Payloads.DecodeFileOffer(encoded.AsSpan(0, encoded.Length - 1)));
    }

    [Fact]
    public void EncodeDecodeChunk_KeepsAllFields()
    {
        var deviceId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        byte[] data = [1, 2, 3];
        var encoded = Payloads.EncodeChunk(deviceId, fileId, 5, data);
        var decoded = Payloads.DecodeChunk(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(fileId, decoded.Value.FileId);
        Assert.Equal(5u, decoded.Value.Index);
        Assert.Equal(data, decoded.Value.Data);
    }

    [Fact]
    public void DecodeChunk_ReturnsNull_WhenBufferIsShorterThanHeader()
    {
        var buffer = new byte[PayloadsConstants.ChunkDataOffset - 1];

        var decoded = Payloads.DecodeChunk(buffer);

        Assert.Null(decoded);
    }

    [Fact]
    public void EncodeDecodeFileOffer_KeepsAllFields()
    {
        var deviceId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        var sha256 = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        var encoded = Payloads.EncodeFileOffer(deviceId, fileId, 123456, sha256, "photo.jpg");
        var decoded = Payloads.DecodeFileOffer(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(fileId, decoded.Value.FileId);
        Assert.Equal(sha256, decoded.Value.Sha256);
        Assert.Equal(123456ul, decoded.Value.Size);
        Assert.Equal("photo.jpg", decoded.Value.Name);
    }

    [Fact]
    public void DecodeFileOffer_ReturnsNull_WhenExtraBytesAtEnd()
    {
        var encoded = Payloads.EncodeFileOffer(
            Guid.NewGuid(),
            Guid.NewGuid(),
            1,
            new byte[32],
            "a.txt"
        );

        Array.Resize(ref encoded, encoded.Length + 1);

        var decoded = Payloads.DecodeFileOffer(encoded);

        Assert.Null(decoded);
    }

    [Fact]
    public void EncodeFileOffer_Throws_WhenSha256IsWrongSize()
    {
        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeFileOffer(Guid.NewGuid(), Guid.NewGuid(), 1, new byte[31], "a.txt")
        );
    }

    [Fact]
    public void EncodeFileOffer_Throws_WhenNameIsTooLong()
    {
        var name = new string('a', PayloadsConstants.MaxNameBytes + 1);

        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeFileOffer(Guid.NewGuid(), Guid.NewGuid(), 1, new byte[32], name)
        );
    }

    [Fact]
    public void FileOffer_UsesUtf8ByteCountForNameLimit()
    {
        var name = new string('а', 127) + "a";
        var encoded = Payloads.EncodeFileOffer(
            Guid.NewGuid(),
            Guid.NewGuid(),
            1,
            new byte[32],
            name
        );
        var decoded = Payloads.DecodeFileOffer(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(255, encoded[PayloadsConstants.NameLengthOffset]);
        Assert.Equal(name, decoded.Value.Name);
        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeFileOffer(
                Guid.NewGuid(),
                Guid.NewGuid(),
                1,
                new byte[32],
                new string('я', 128)
            )
        );
    }
}
