namespace Protocol.Tests;

public class PayloadsTests
{
    [Fact]
    public void EncodeDecodeChunk_KeepsAllFields()
    {
        var fileId = Guid.NewGuid();
        byte[] data = [1, 2, 3];
        var encoded = Payloads.EncodeChunk(7, fileId, 5, data);
        var decoded = Payloads.DecodeChunk(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(7, decoded.Value.PeerId);
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
        var fileId = Guid.NewGuid();
        var sha256 = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        var encoded = Payloads.EncodeFileOffer(3, fileId, 123456, sha256, "photo.jpg");
        var decoded = Payloads.DecodeFileOffer(encoded);

        Assert.NotNull(decoded);
        Assert.Equal(3, decoded.Value.PeerId);
        Assert.Equal(fileId, decoded.Value.FileId);
        Assert.Equal(sha256, decoded.Value.Sha256);
        Assert.Equal(123456u, decoded.Value.Size);
        Assert.Equal("photo.jpg", decoded.Value.Name);
    }

    [Fact]
    public void DecodeFileOffer_ReturnsNull_WhenExtraBytesAtEnd()
    {
        var encoded = Payloads.EncodeFileOffer(3, Guid.NewGuid(), 1, new byte[32], "a.txt");

        Array.Resize(ref encoded, encoded.Length + 1);

        var decoded = Payloads.DecodeFileOffer(encoded);

        Assert.Null(decoded);
    }

    [Fact]
    public void EncodeFileOffer_Throws_WhenSha256IsWrongSize()
    {
        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeFileOffer(3, Guid.NewGuid(), 1, new byte[31], "a.txt")
        );
    }

    [Fact]
    public void EncodeFileOffer_Throws_WhenNameIsTooLongs()
    {
        var name = new string('a', PayloadsConstants.MaxNameBytes + 1);

        Assert.Throws<ArgumentException>(() =>
            Payloads.EncodeFileOffer(3, Guid.NewGuid(), 1, new byte[31], name)
        );
    }
}
