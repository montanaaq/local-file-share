using System.Buffers.Binary;

namespace Protocol.Tests;

public class PayloadsTests
{
    [Fact]
    public void EncodeDecodeChunk_ProducesExpectedBytesAndFields()
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        var fileId = Guid.Parse("FFEEDDCCBBAA99887766554433221100");
        uint index = 0x01020304u;
        byte[] data = [0xAA, 0xBB];
        byte[] expected = Convert.FromHexString(
            "00112233445566778899AABBCCDDEEFF" + "FFEEDDCCBBAA99887766554433221100" + "01020304AABB"
        );

        byte[] encoded = Payloads.EncodeChunk(deviceId, fileId, index, data);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeChunk(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(fileId, decoded.Value.FileId);
        Assert.Equal(index, decoded.Value.Index);
        Assert.Equal(data, decoded.Value.Data);
    }

    [Fact]
    public void EncodeDecodeAck_ProducesExpectedBytesAndFields()
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        uint ackedSeq = 0x01020304u;
        byte[] expected = Convert.FromHexString("00112233445566778899AABBCCDDEEFF01020304");

        byte[] encoded = Payloads.EncodeAck(deviceId, ackedSeq);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeAck(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(ackedSeq, decoded.Value.AckedSeq);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(19)]
    [InlineData(21)]
    public void DecodeAck_ReturnsNull_WhenLengthIsInvalid(int length)
    {
        byte[] payload = new byte[length];
        var decoded = Payloads.DecodeAck(payload);

        Assert.Null(decoded);
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
    public void EncodeDecodeChunk_FitsMaximumPacket_WhenDataIs1024Bytes()
    {
        byte[] encoded = Payloads.EncodeChunk(Guid.NewGuid(), Guid.NewGuid(), 0, new byte[1024]);
        var decoded = Payloads.DecodeChunk(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(1024, decoded.Value.Data.Length);
        Assert.Equal(1060, encoded.Length);
        Assert.Equal(1072, new Packet(MessageType.Chunk, 0, encoded).Serialize().Length);
    }

    [Fact]
    public void EncodeDecodeFileOffer_ProducesExpectedBytesAndFields()
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        var fileId = Guid.Parse("FFEEDDCCBBAA99887766554433221100");
        ulong size = 0x010203ul;
        byte[] sha256 = new byte[32];
        string name = "a.txt";
        byte[] expected = Convert.FromHexString(
            "00112233445566778899AABBCCDDEEFF"
                + "FFEEDDCCBBAA99887766554433221100"
                + "0000000000010203"
                + new string('0', 64)
                + "05612E747874"
        );

        byte[] encoded = Payloads.EncodeFileOffer(deviceId, fileId, size, sha256, name);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeFileOffer(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(fileId, decoded.Value.FileId);
        Assert.Equal(size, decoded.Value.Size);
        Assert.Equal(sha256, decoded.Value.Sha256);
        Assert.Equal(name, decoded.Value.Name);
    }

    [Theory]
    [InlineData(0ul)]
    [InlineData(20971520ul)]
    public void EncodeDecodeFileOffer_KeepsBoundarySizeAndUtf8Name(ulong size)
    {
        byte[] encoded = Payloads.EncodeFileOffer(
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
        byte[] encoded = Payloads.EncodeFileOffer(
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
        byte[] encoded = Payloads.EncodeFileOffer(
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
        byte[] encoded = Payloads.EncodeFileOffer(
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
        byte[] encoded = Payloads.EncodeChunk(deviceId, fileId, 5, data);
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
        byte[] payload = new byte[PayloadsConstants.ChunkDataOffset - 1];

        var decoded = Payloads.DecodeChunk(payload);

        Assert.Null(decoded);
    }

    [Fact]
    public void EncodeDecodeFileOffer_KeepsAllFields()
    {
        var deviceId = Guid.NewGuid();
        var fileId = Guid.NewGuid();
        byte[] sha256 = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        byte[] encoded = Payloads.EncodeFileOffer(deviceId, fileId, 123456, sha256, "photo.jpg");
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
        byte[] encoded = Payloads.EncodeFileOffer(
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
        string name = new string('a', PayloadsConstants.MaxNameBytes + 1);

        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeFileOffer(Guid.NewGuid(), Guid.NewGuid(), 1, new byte[32], name)
        );
    }

    [Fact]
    public void FileOffer_UsesUtf8ByteCountForNameLimit()
    {
        string name = new string('а', 127) + "a";
        byte[] encoded = Payloads.EncodeFileOffer(
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

    [Fact]
    public void EncodeDecodeHeartbeat_ProducesExpectedBytesAndFields()
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        byte[] expected = Convert.FromHexString("00112233445566778899AABBCCDDEEFF");

        byte[] encoded = Payloads.EncodeHeartbeat(deviceId);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeHeartbeat(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(15)]
    [InlineData(17)]
    public void DecodeHeartbeat_ReturnsNull_WhenLengthIsInvalid(int length)
    {
        byte[] payload = new byte[length];

        var decoded = Payloads.DecodeHeartbeat(payload);

        Assert.Null(decoded);
    }

    [Fact]
    public void EncodeDecodePeerInfo_ProducesExpectedBytesAndFields()
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        byte[] expected = Convert.FromHexString("00112233445566778899AABBCCDDEEFF" + "025043");

        byte[] encoded = Payloads.EncodePeerInfo(deviceId, "PC");

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodePeerInfo(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal("PC", decoded.Value.Name);
    }

    [Theory]
    [InlineData("")]
    [InlineData("00")]
    [InlineData("0241")]
    [InlineData("014142")]
    [InlineData("01FF")]
    public void DecodePeerInfo_ReturnsNull_WhenPayloadIsInvalid(string suffix)
    {
        byte[] payload = Convert.FromHexString("00112233445566778899AABBCCDDEEFF" + suffix);

        Assert.Null(Payloads.DecodePeerInfo(payload));
    }

    [Fact]
    public void PeerInfo_UsesUtf8ByteCountForNameLimit()
    {
        var deviceId = Guid.NewGuid();
        string name = new string('а', 127) + "a";

        byte[] encoded = Payloads.EncodePeerInfo(deviceId, name);
        var decoded = Payloads.DecodePeerInfo(encoded);

        Assert.Equal(255, encoded[PayloadsConstants.PeerNameLengthOffset]);
        Assert.NotNull(decoded);
        Assert.Equal(name, decoded.Value.Name);

        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodePeerInfo(deviceId, new string('а', 128))
        );
        Assert.Throws<ArgumentException>(() => Payloads.EncodePeerInfo(deviceId, ""));
    }

    [Theory]
    [InlineData(true, "01")]
    [InlineData(false, "00")]
    public void EncodeDecodeFileAnswer_ProducesExpectedBytesAndFields(
        bool accepted,
        string acceptedHex
    )
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        var fileId = Guid.Parse("FFEEDDCCBBAA99887766554433221100");
        uint nextIndex = 0x01020304u;

        byte[] expected = Convert.FromHexString(
            "00112233445566778899AABBCCDDEEFF"
                + "FFEEDDCCBBAA99887766554433221100"
                + acceptedHex
                + "01020304"
        );

        byte[] encoded = Payloads.EncodeFileAnswer(deviceId, fileId, accepted, nextIndex);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeFileAnswer(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(fileId, decoded.Value.FileId);
        Assert.Equal(accepted, decoded.Value.Accepted);
        Assert.Equal(nextIndex, decoded.Value.NextIndex);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(32)]
    [InlineData(36)]
    [InlineData(38)]
    public void DecodeFileAnswer_ReturnsNull_WhenLengthIsInvalid(int length)
    {
        byte[] payload = new byte[length];
        var decoded = Payloads.DecodeFileAnswer(payload);

        Assert.Null(decoded);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(255)]
    public void DecodeFileAnswer_ReturnsNull_WhenAcceptedIsInvalid(byte accepted)
    {
        byte[] payload = Payloads.EncodeFileAnswer(Guid.NewGuid(), Guid.NewGuid(), true, 0);

        payload[PayloadsConstants.AcceptedOffset] = accepted;

        Assert.Null(Payloads.DecodeFileAnswer(payload));
    }

    [Fact]
    public void EncodeDecodeFileDone_ProducesExpectedBytesAndFields()
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        var fileId = Guid.Parse("FFEEDDCCBBAA99887766554433221100");
        byte[] expected = Convert.FromHexString(
            "00112233445566778899AABBCCDDEEFF" + "FFEEDDCCBBAA99887766554433221100"
        );

        byte[] encoded = Payloads.EncodeFileDone(deviceId, fileId);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeFileDone(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(fileId, decoded.Value.FileId);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(16)]
    [InlineData(31)]
    [InlineData(33)]
    public void DecodeFileDone_ReturnsNull_WhenLengthIsInvalid(int length)
    {
        byte[] payload = new byte[length];
        var decoded = Payloads.DecodeFileDone(payload);

        Assert.Null(decoded);
    }

    [Theory]
    [InlineData(true, "01")]
    [InlineData(false, "00")]
    public void EncodeDecodeFileResult_ProducesExpectedBytesAndFields(bool ok, string okHex)
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        var fileId = Guid.Parse("FFEEDDCCBBAA99887766554433221100");
        byte[] expected = Convert.FromHexString(
            "00112233445566778899AABBCCDDEEFF" + "FFEEDDCCBBAA99887766554433221100" + okHex
        );

        byte[] encoded = Payloads.EncodeFileResult(deviceId, fileId, ok);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeFileResult(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(fileId, decoded.Value.FileId);
        Assert.Equal(ok, decoded.Value.Ok);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(32)]
    [InlineData(34)]
    public void DecodeFileResult_ReturnsNull_WhenLengthIsInvalid(int length)
    {
        byte[] payload = new byte[length];
        var decoded = Payloads.DecodeFileResult(payload);

        Assert.Null(decoded);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(255)]
    public void DecodeFileResult_ReturnsNull_WhenOkIsInvalid(byte ok)
    {
        byte[] payload = Payloads.EncodeFileResult(Guid.NewGuid(), Guid.NewGuid(), true);

        payload[PayloadsConstants.OkOffset] = ok;

        Assert.Null(Payloads.DecodeFileResult(payload));
    }

    [Theory]
    [InlineData("Hi", "4869")]
    [InlineData("", "")]
    [InlineData("Привет", "D09FD180D0B8D0B2D0B5D182")]
    public void EncodeDecodeClipboard_ProducesExpectedBytesAndFields(string text, string textHex)
    {
        var deviceId = Guid.Parse("00112233445566778899AABBCCDDEEFF");
        byte[] expected = Convert.FromHexString("00112233445566778899AABBCCDDEEFF" + textHex);

        byte[] encoded = Payloads.EncodeClipboard(deviceId, text);

        Assert.Equal(expected, encoded);

        var decoded = Payloads.DecodeClipboard(expected);

        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(text, decoded.Value.Text);
    }

    [Fact]
    public void Clipboard_UsesUtf8ByteCountForTextLimit()
    {
        var deviceId = Guid.NewGuid();
        string text = new string('а', 500);

        byte[] encoded = Payloads.EncodeClipboard(deviceId, text);
        var decoded = Payloads.DecodeClipboard(encoded);

        Assert.Equal(1016, encoded.Length);
        Assert.NotNull(decoded);
        Assert.Equal(deviceId, decoded.Value.DeviceId);
        Assert.Equal(text, decoded.Value.Text);

        Assert.Throws<ArgumentException>(() => Payloads.EncodeClipboard(deviceId, text + "a"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(15)]
    [InlineData(1017)]
    public void DecodeClipboard_ReturnsNull_WhenLengthIsInvalid(int length)
    {
        byte[] payload = new byte[length];
        var decoded = Payloads.DecodeClipboard(payload);

        Assert.Null(decoded);
    }

    [Theory]
    [InlineData("FF")]
    [InlineData("D0")]
    public void DecodeClipboard_ReturnsNull_WhenTextIsInvalidUtf8(string textHex)
    {
        byte[] payload = Convert.FromHexString("00112233445566778899AABBCCDDEEFF" + textHex);
        var decoded = Payloads.DecodeClipboard(payload);

        Assert.Null(decoded);
    }
}
