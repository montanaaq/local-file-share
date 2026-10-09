using System;
using System.Buffers.Binary;
using System.Text;

namespace Protocol;

public static class Payloads
{
    public static byte[] EncodeChunk(
        ushort peerId,
        Guid fileId,
        uint index,
        ReadOnlySpan<byte> data
    )
    {
        var buf = new byte[PayloadsConstants.ChunkDataOffset + data.Length];
        BinaryPrimitives.WriteUInt16BigEndian(buf, peerId);
        fileId.TryWriteBytes(buf.AsSpan(PayloadsConstants.FileIdOffset));
        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(PayloadsConstants.IndexOffset), index);
        data.CopyTo(buf.AsSpan(PayloadsConstants.ChunkDataOffset));
        return buf;
    }

    public static (ushort PeerId, Guid FileId, uint Index, byte[] Data)? DecodeChunk(
        ReadOnlySpan<byte> p
    )
    {
        if (p.Length < PayloadsConstants.ChunkDataOffset)
            return null;

        return (
            BinaryPrimitives.ReadUInt16BigEndian(p),
            new Guid(p.Slice(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize)),
            BinaryPrimitives.ReadUInt32BigEndian(p[PayloadsConstants.IndexOffset..]),
            p[PayloadsConstants.ChunkDataOffset..].ToArray()
        );
    }

    public static byte[] EncodeFileOffer(
        ushort peerId,
        Guid fileId,
        ulong size,
        byte[] sha256,
        string name
    )
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);

        if (sha256.Length != PayloadsConstants.Sha256Size)
            throw new ArgumentException("SHA-256 must be 32 bytes", nameof(sha256));
        if (nameBytes.Length > PayloadsConstants.MaxNameBytes)
            throw new ArgumentException("File name is too long", nameof(name));

        var buf = new byte[PayloadsConstants.NameOffset + nameBytes.Length];
        BinaryPrimitives.WriteUInt16BigEndian(buf, peerId);
        fileId.TryWriteBytes(buf.AsSpan(PayloadsConstants.FileIdOffset));
        BinaryPrimitives.WriteUInt64BigEndian(buf.AsSpan(PayloadsConstants.FileSizeOffset), size);
        sha256.CopyTo(buf, PayloadsConstants.Sha256Offset);
        buf[PayloadsConstants.NameLengthOffset] = (byte)nameBytes.Length;
        nameBytes.CopyTo(buf, PayloadsConstants.NameOffset);
        return buf;
    }

    public static (
        ushort PeerId,
        Guid FileId,
        ulong Size,
        byte[] Sha256,
        string Name
    )? DecodeFileOffer(ReadOnlySpan<byte> p)
    {
        if (p.Length < PayloadsConstants.NameOffset)
            return null;

        var nameLength = p[PayloadsConstants.NameLengthOffset];
        if (p.Length != PayloadsConstants.NameOffset + nameLength)
            return null;

        return (
            BinaryPrimitives.ReadUInt16BigEndian(p),
            new Guid(p.Slice(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize)),
            BinaryPrimitives.ReadUInt64BigEndian(p[PayloadsConstants.FileSizeOffset..]),
            p.Slice(PayloadsConstants.Sha256Offset, PayloadsConstants.Sha256Size).ToArray(),
            Encoding.UTF8.GetString(p.Slice(PayloadsConstants.NameOffset, nameLength))
        );
    }
}
