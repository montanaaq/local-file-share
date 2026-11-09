using System.Buffers.Binary;

namespace Protocol.Tests;

public class PayloadsTests
{
    [Theory]
    [InlineData(0ul)]
    [InlineData((ulong)ProtocolConstants.MaxFileSize)]
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
                (ulong)ProtocolConstants.MaxFileSize + 1,
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
            (ulong)ProtocolConstants.MaxFileSize + 1
        );
        Assert.Null(Payloads.DecodeFileOffer(encoded));
    }
}
