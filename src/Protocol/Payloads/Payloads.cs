using System;
using System.Buffers.Binary;
using System.Text;

namespace Protocol;

public static class Payloads
{
    private static readonly UTF8Encoding Utf8 = new(false, true);

    public static byte[] EncodeAck(Guid deviceId, uint ackedSeq)
    {
        var buf = new byte[PayloadsConstants.AckSize];
        deviceId.TryWriteBytes(buf.AsSpan(0, PayloadsConstants.DeviceIdSize), true, out _);
        BinaryPrimitives.WriteUInt32BigEndian(
            buf.AsSpan(PayloadsConstants.AckedSeqOffset),
            ackedSeq
        );
        return buf;
    }

    public static (Guid DeviceId, uint AckedSeq)? DecodeAck(ReadOnlySpan<byte> p)
    {
        if (p.Length != PayloadsConstants.AckSize)
            return null;

        return (
            new Guid(p[..PayloadsConstants.DeviceIdSize], true),
            BinaryPrimitives.ReadUInt32BigEndian(p[PayloadsConstants.AckedSeqOffset..])
        );
    }

    public static byte[] EncodeChunk(
        Guid deviceId,
        Guid fileId,
        uint index,
        ReadOnlySpan<byte> data
    )
    {
        if (data.Length > ProtocolConstants.ChunkSize)
            throw new ArgumentException("Chunk data exceeds 1024 bytes", nameof(data));

        var buf = new byte[PayloadsConstants.ChunkDataOffset + data.Length];
        deviceId.TryWriteBytes(buf.AsSpan(0, PayloadsConstants.DeviceIdSize), true, out _);
        fileId.TryWriteBytes(
            buf.AsSpan(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize),
            true,
            out _
        );
        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(PayloadsConstants.IndexOffset), index);
        data.CopyTo(buf.AsSpan(PayloadsConstants.ChunkDataOffset));
        return buf;
    }

    public static (Guid DeviceId, Guid FileId, uint Index, byte[] Data)? DecodeChunk(
        ReadOnlySpan<byte> p
    )
    {
        if (p.Length < PayloadsConstants.ChunkDataOffset || p.Length > Packet.MaxPayloadSize)
            return null;

        return (
            new Guid(p[..PayloadsConstants.DeviceIdSize], true),
            new Guid(p.Slice(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize), true),
            BinaryPrimitives.ReadUInt32BigEndian(p[PayloadsConstants.IndexOffset..]),
            p[PayloadsConstants.ChunkDataOffset..].ToArray()
        );
    }

    public static byte[] EncodeFileOffer(
        Guid deviceId,
        Guid fileId,
        ulong size,
        byte[] sha256,
        string name
    )
    {
        var nameBytes = Utf8.GetBytes(name);

        if (sha256.Length != PayloadsConstants.Sha256Size)
            throw new ArgumentException("SHA-256 must be 32 bytes", nameof(sha256));
        if (nameBytes.Length == 0 || nameBytes.Length > PayloadsConstants.MaxNameBytes)
            throw new ArgumentException(
                "File name must contain 1 to 255 UTF-8 bytes",
                nameof(name)
            );
        if (size > (ulong)ProtocolConstants.MaxFileSize)
            throw new ArgumentOutOfRangeException(nameof(size), "File exceeds size limit");

        var buf = new byte[PayloadsConstants.NameOffset + nameBytes.Length];
        deviceId.TryWriteBytes(buf.AsSpan(0, PayloadsConstants.DeviceIdSize), true, out _);
        fileId.TryWriteBytes(
            buf.AsSpan(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize),
            true,
            out _
        );
        BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(PayloadsConstants.FileSizeOffset), size);
        sha256.CopyTo(buf, PayloadsConstants.Sha256Offset);
        buf[PayloadsConstants.NameLengthOffset] = (byte)nameBytes.Length;
        nameBytes.CopyTo(buf, PayloadsConstants.NameOffset);
        return buf;
    }

    public static (
        Guid DeviceId,
        Guid FileId,
        ulong Size,
        byte[] Sha256,
        string Name
    )? DecodeFileOffer(ReadOnlySpan<byte> p)
    {
        if (p.Length < PayloadsConstants.NameOffset)
            return null;

        var nameLength = p[PayloadsConstants.NameLengthOffset];
        if (nameLength == 0 || p.Length != PayloadsConstants.NameOffset + nameLength)
            return null;

        var size = BinaryPrimitives.ReadUInt64BigEndian(p[PayloadsConstants.FileSizeOffset..]);
        if (size > (ulong)ProtocolConstants.MaxFileSize)
            return null;

        string name;
        try
        {
            name = Utf8.GetString(p.Slice(PayloadsConstants.NameOffset, nameLength));
        }
        catch (DecoderFallbackException)
        {
            return null;
        }

        return (
            new Guid(p[..PayloadsConstants.DeviceIdSize], true),
            new Guid(p.Slice(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize), true),
            size,
            p.Slice(PayloadsConstants.Sha256Offset, PayloadsConstants.Sha256Size).ToArray(),
            name
        );
    }
}
