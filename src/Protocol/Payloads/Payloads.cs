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
        deviceId.TryWriteBytes(
            buf.AsSpan(0, PayloadsConstants.DeviceIdSize),
            bigEndian: true,
            out _
        );
        BinaryPrimitives.WriteUInt32BigEndian(
            buf.AsSpan(PayloadsConstants.AckedSeqOffset),
            ackedSeq
        );
        return buf;
    }

    public static (Guid DeviceId, uint AckedSeq)? DecodeAck(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != PayloadsConstants.AckSize)
            return null;

        var deviceId = new Guid(payload[..PayloadsConstants.DeviceIdSize], bigEndian: true);
        var ackedSeq = BinaryPrimitives.ReadUInt32BigEndian(
            payload[PayloadsConstants.AckedSeqOffset..]
        );

        return (deviceId, ackedSeq);
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
        WriteFileIds(buf, deviceId, fileId);

        BinaryPrimitives.WriteUInt32BigEndian(buf.AsSpan(PayloadsConstants.IndexOffset), index);
        data.CopyTo(buf.AsSpan(PayloadsConstants.ChunkDataOffset));
        return buf;
    }

    public static (Guid DeviceId, Guid FileId, uint Index, byte[] Data)? DecodeChunk(
        ReadOnlySpan<byte> payload
    )
    {
        if (
            payload.Length < PayloadsConstants.ChunkDataOffset
            || payload.Length > Packet.MaxPayloadSize
        )
            return null;

        var (deviceId, fileId) = GetFileIds(payload);
        var index = BinaryPrimitives.ReadUInt32BigEndian(payload[PayloadsConstants.IndexOffset..]);
        var data = payload[PayloadsConstants.ChunkDataOffset..].ToArray();

        return (deviceId, fileId, index, data);
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

        if (size > ProtocolConstants.MaxFileSize)
            throw new ArgumentOutOfRangeException(nameof(size), "File exceeds size limit");

        var buf = new byte[PayloadsConstants.NameOffset + nameBytes.Length];
        WriteFileIds(buf, deviceId, fileId);

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
    )? DecodeFileOffer(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < PayloadsConstants.NameOffset)
            return null;

        var nameLength = payload[PayloadsConstants.NameLengthOffset];
        if (nameLength == 0 || payload.Length != PayloadsConstants.NameOffset + nameLength)
            return null;

        var size = BinaryPrimitives.ReadUInt64BigEndian(
            payload[PayloadsConstants.FileSizeOffset..]
        );
        if (size > ProtocolConstants.MaxFileSize)
            return null;

        string name;
        try
        {
            name = Utf8.GetString(payload.Slice(PayloadsConstants.NameOffset, nameLength));
        }
        catch (DecoderFallbackException)
        {
            return null;
        }

        var (deviceId, fileId) = GetFileIds(payload);
        var sha256 = payload
            .Slice(PayloadsConstants.Sha256Offset, PayloadsConstants.Sha256Size)
            .ToArray();

        return (deviceId, fileId, size, sha256, name);
    }

    public static byte[] EncodeHeartbeat(Guid deviceId)
    {
        var buf = new byte[PayloadsConstants.DeviceIdSize];
        deviceId.TryWriteBytes(buf, bigEndian: true, out _);
        return buf;
    }

    public static Guid? DecodeHeartbeat(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != PayloadsConstants.DeviceIdSize)
            return null;

        return new Guid(payload, bigEndian: true);
    }

    public static byte[] EncodePeerInfo(Guid deviceId, string name)
    {
        var nameBytes = Utf8.GetBytes(name);
        if (nameBytes.Length < 1 || nameBytes.Length > PayloadsConstants.MaxNameBytes)
        {
            throw new ArgumentException(
                "Device name must contain 1 to 255 UTF-8 bytes",
                nameof(name)
            );
        }

        var buf = new byte[PayloadsConstants.PeerNameOffset + nameBytes.Length];
        buf[PayloadsConstants.PeerNameLengthOffset] = (byte)nameBytes.Length;
        deviceId.TryWriteBytes(
            buf.AsSpan(0, PayloadsConstants.DeviceIdSize),
            bigEndian: true,
            out _
        );

        nameBytes.CopyTo(buf, PayloadsConstants.PeerNameOffset);
        return buf;
    }

    public static (Guid DeviceId, string Name)? DecodePeerInfo(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < PayloadsConstants.PeerNameOffset)
            return null;

        var nameLength = payload[PayloadsConstants.PeerNameLengthOffset];
        if (nameLength == 0 || payload.Length != PayloadsConstants.PeerNameOffset + nameLength)
            return null;

        string name;
        try
        {
            name = Utf8.GetString(payload.Slice(PayloadsConstants.PeerNameOffset, nameLength));
        }
        catch (DecoderFallbackException)
        {
            return null;
        }

        var deviceId = new Guid(payload[..PayloadsConstants.DeviceIdSize], bigEndian: true);

        return (deviceId, name);
    }

    public static byte[] EncodeFileAnswer(Guid deviceId, Guid fileId, bool accepted, uint nextIndex)
    {
        var buf = new byte[PayloadsConstants.FileAnswerSize];
        WriteFileIds(buf, deviceId, fileId);

        buf[PayloadsConstants.AcceptedOffset] = accepted ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32BigEndian(
            buf.AsSpan(PayloadsConstants.NextIndexOffset),
            nextIndex
        );

        return buf;
    }

    public static (Guid DeviceId, Guid FileId, bool Accepted, uint NextIndex)? DecodeFileAnswer(
        ReadOnlySpan<byte> payload
    )
    {
        if (payload.Length != PayloadsConstants.FileAnswerSize)
            return null;
        if (payload[PayloadsConstants.AcceptedOffset] > 1)
            return null;

        var (deviceId, fileId) = GetFileIds(payload);
        var accepted = payload[PayloadsConstants.AcceptedOffset] == 1;
        var nextIndex = BinaryPrimitives.ReadUInt32BigEndian(
            payload[PayloadsConstants.NextIndexOffset..]
        );

        return (deviceId, fileId, accepted, nextIndex);
    }

    public static byte[] EncodeFileDone(Guid deviceId, Guid fileId)
    {
        var buf = new byte[PayloadsConstants.FileDoneSize];
        WriteFileIds(buf, deviceId, fileId);
        return buf;
    }

    public static (Guid DeviceId, Guid FileId)? DecodeFileDone(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != PayloadsConstants.FileDoneSize)
            return null;

        var (deviceId, fileId) = GetFileIds(payload);

        return (deviceId, fileId);
    }

    public static byte[] EncodeFileResult(Guid deviceId, Guid fileId, bool ok)
    {
        var buf = new byte[PayloadsConstants.FileResultSize];
        WriteFileIds(buf, deviceId, fileId);
        buf[PayloadsConstants.OkOffset] = ok ? (byte)1 : (byte)0;

        return buf;
    }

    public static (Guid DeviceId, Guid FileId, bool Ok)? DecodeFileResult(
        ReadOnlySpan<byte> payload
    )
    {
        if (payload.Length != PayloadsConstants.FileResultSize)
            return null;
        if (payload[PayloadsConstants.OkOffset] > 1)
            return null;

        var (deviceId, fileId) = GetFileIds(payload);
        var ok = payload[PayloadsConstants.OkOffset] == 1;

        return (deviceId, fileId, ok);
    }

    private static (Guid DeviceId, Guid FileId) GetFileIds(ReadOnlySpan<byte> payload)
    {
        return (
            new Guid(payload[..PayloadsConstants.DeviceIdSize], bigEndian: true),
            new Guid(
                payload.Slice(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize),
                bigEndian: true
            )
        );
    }

    private static void WriteFileIds(byte[] buf, Guid deviceId, Guid fileId)
    {
        deviceId.TryWriteBytes(
            buf.AsSpan(0, PayloadsConstants.DeviceIdSize),
            bigEndian: true,
            out _
        );
        fileId.TryWriteBytes(
            buf.AsSpan(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize),
            bigEndian: true,
            out _
        );
    }
}
