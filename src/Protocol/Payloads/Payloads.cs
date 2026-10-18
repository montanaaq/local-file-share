using System;
using System.Buffers.Binary;
using System.Text;

namespace Protocol;

public static class Payloads
{
    private static readonly UTF8Encoding Utf8 = new(false, true);

    public static byte[] EncodeAck(Guid deviceId, uint ackedSeq)
    {
        byte[] buffer = new byte[PayloadsConstants.AckSize];
        deviceId.TryWriteBytes(
            buffer.AsSpan(0, PayloadsConstants.DeviceIdSize),
            bigEndian: true,
            out _
        );
        BinaryPrimitives.WriteUInt32BigEndian(
            buffer.AsSpan(PayloadsConstants.AckedSeqOffset),
            ackedSeq
        );
        return buffer;
    }

    public static (Guid DeviceId, uint AckedSeq)? DecodeAck(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != PayloadsConstants.AckSize)
        {
            return null;
        }

        var deviceId = new Guid(payload[..PayloadsConstants.DeviceIdSize], bigEndian: true);
        uint ackedSeq = BinaryPrimitives.ReadUInt32BigEndian(
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
        {
            throw new ArgumentException("Chunk data exceeds 1024 bytes", nameof(data));
        }

        byte[] buffer = new byte[PayloadsConstants.ChunkDataOffset + data.Length];
        WriteFileIds(buffer, deviceId, fileId);

        BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(PayloadsConstants.IndexOffset), index);
        data.CopyTo(buffer.AsSpan(PayloadsConstants.ChunkDataOffset));
        return buffer;
    }

    public static (Guid DeviceId, Guid FileId, uint Index, byte[] Data)? DecodeChunk(
        ReadOnlySpan<byte> payload
    )
    {
        if (
            payload.Length < PayloadsConstants.ChunkDataOffset
            || payload.Length > Packet.MaxPayloadSize
        )
        {
            return null;
        }

        var (deviceId, fileId) = GetFileIds(payload);
        uint index = BinaryPrimitives.ReadUInt32BigEndian(payload[PayloadsConstants.IndexOffset..]);
        byte[] data = payload[PayloadsConstants.ChunkDataOffset..].ToArray();

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
        byte[] nameBytes = Utf8.GetBytes(name);

        if (sha256.Length != PayloadsConstants.Sha256Size)
        {
            throw new ArgumentException("SHA-256 must contain 32 bytes", nameof(sha256));
        }

        if (nameBytes.Length == 0 || nameBytes.Length > PayloadsConstants.MaxNameBytes)
        {
            throw new ArgumentException(
                "File name must contain 1 to 255 UTF-8 bytes",
                nameof(name)
            );
        }

        if (size > ProtocolConstants.MaxFileSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(size),
                size,
                "File size exceeds the maximum allowed size"
            );
        }

        byte[] buffer = new byte[PayloadsConstants.NameOffset + nameBytes.Length];
        WriteFileIds(buffer, deviceId, fileId);

        BinaryPrimitives.WriteUInt64BigEndian(
            buffer.AsSpan(PayloadsConstants.FileSizeOffset),
            size
        );
        sha256.CopyTo(buffer, PayloadsConstants.Sha256Offset);
        buffer[PayloadsConstants.NameLengthOffset] = (byte)nameBytes.Length;
        nameBytes.CopyTo(buffer, PayloadsConstants.NameOffset);
        return buffer;
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
        {
            return null;
        }

        byte nameLength = payload[PayloadsConstants.NameLengthOffset];
        if (nameLength == 0 || payload.Length != PayloadsConstants.NameOffset + nameLength)
        {
            return null;
        }

        ulong size = BinaryPrimitives.ReadUInt64BigEndian(
            payload[PayloadsConstants.FileSizeOffset..]
        );
        if (size > ProtocolConstants.MaxFileSize)
        {
            return null;
        }

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
        byte[] sha256 = payload
            .Slice(PayloadsConstants.Sha256Offset, PayloadsConstants.Sha256Size)
            .ToArray();

        return (deviceId, fileId, size, sha256, name);
    }

    public static byte[] EncodeHeartbeat(Guid deviceId)
    {
        byte[] buffer = new byte[PayloadsConstants.DeviceIdSize];
        deviceId.TryWriteBytes(buffer, bigEndian: true, out _);
        return buffer;
    }

    public static Guid? DecodeHeartbeat(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != PayloadsConstants.DeviceIdSize)
        {
            return null;
        }

        return new Guid(payload, bigEndian: true);
    }

    public static byte[] EncodePeerInfo(Guid deviceId, string name)
    {
        byte[] nameBytes = Utf8.GetBytes(name);
        if (nameBytes.Length is < 1 or > PayloadsConstants.MaxNameBytes)
        {
            throw new ArgumentException(
                "Device name must contain 1 to 255 UTF-8 bytes",
                nameof(name)
            );
        }

        byte[] buffer = new byte[PayloadsConstants.PeerNameOffset + nameBytes.Length];
        buffer[PayloadsConstants.PeerNameLengthOffset] = (byte)nameBytes.Length;
        deviceId.TryWriteBytes(
            buffer.AsSpan(0, PayloadsConstants.DeviceIdSize),
            bigEndian: true,
            out _
        );

        nameBytes.CopyTo(buffer, PayloadsConstants.PeerNameOffset);
        return buffer;
    }

    public static (Guid DeviceId, string Name)? DecodePeerInfo(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < PayloadsConstants.PeerNameOffset)
        {
            return null;
        }

        byte nameLength = payload[PayloadsConstants.PeerNameLengthOffset];
        if (nameLength == 0 || payload.Length != PayloadsConstants.PeerNameOffset + nameLength)
        {
            return null;
        }

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
        byte[] buffer = new byte[PayloadsConstants.FileAnswerSize];
        WriteFileIds(buffer, deviceId, fileId);

        buffer[PayloadsConstants.AcceptedOffset] = accepted ? (byte)1 : (byte)0;
        BinaryPrimitives.WriteUInt32BigEndian(
            buffer.AsSpan(PayloadsConstants.NextIndexOffset),
            nextIndex
        );

        return buffer;
    }

    public static (Guid DeviceId, Guid FileId, bool Accepted, uint NextIndex)? DecodeFileAnswer(
        ReadOnlySpan<byte> payload
    )
    {
        if (payload.Length != PayloadsConstants.FileAnswerSize)
        {
            return null;
        }

        if (payload[PayloadsConstants.AcceptedOffset] > 1)
        {
            return null;
        }

        var (deviceId, fileId) = GetFileIds(payload);
        bool accepted = payload[PayloadsConstants.AcceptedOffset] == 1;
        uint nextIndex = BinaryPrimitives.ReadUInt32BigEndian(
            payload[PayloadsConstants.NextIndexOffset..]
        );

        return (deviceId, fileId, accepted, nextIndex);
    }

    public static byte[] EncodeFileDone(Guid deviceId, Guid fileId)
    {
        byte[] buffer = new byte[PayloadsConstants.FileDoneSize];
        WriteFileIds(buffer, deviceId, fileId);
        return buffer;
    }

    public static (Guid DeviceId, Guid FileId)? DecodeFileDone(ReadOnlySpan<byte> payload)
    {
        if (payload.Length != PayloadsConstants.FileDoneSize)
        {
            return null;
        }

        var (deviceId, fileId) = GetFileIds(payload);

        return (deviceId, fileId);
    }

    public static byte[] EncodeFileResult(Guid deviceId, Guid fileId, bool ok)
    {
        byte[] buffer = new byte[PayloadsConstants.FileResultSize];
        WriteFileIds(buffer, deviceId, fileId);
        buffer[PayloadsConstants.OkOffset] = ok ? (byte)1 : (byte)0;

        return buffer;
    }

    public static (Guid DeviceId, Guid FileId, bool Ok)? DecodeFileResult(
        ReadOnlySpan<byte> payload
    )
    {
        if (payload.Length != PayloadsConstants.FileResultSize)
        {
            return null;
        }

        if (payload[PayloadsConstants.OkOffset] > 1)
        {
            return null;
        }

        var (deviceId, fileId) = GetFileIds(payload);
        bool ok = payload[PayloadsConstants.OkOffset] == 1;

        return (deviceId, fileId, ok);
    }

    public static byte[] EncodeClipboard(Guid deviceId, string text)
    {
        byte[] textBytes = Utf8.GetBytes(text);
        if (textBytes.Length > ProtocolConstants.ClipboardTextSize)
        {
            throw new ArgumentException(
                "Clipboard text must not exceed 1000 UTF-8 bytes",
                nameof(text)
            );
        }
        byte[] buffer = new byte[PayloadsConstants.DeviceIdSize + textBytes.Length];
        deviceId.TryWriteBytes(buffer, bigEndian: true, out _);
        textBytes.CopyTo(buffer, PayloadsConstants.DeviceIdSize);

        return buffer;
    }

    public static (Guid DeviceId, string Text)? DecodeClipboard(ReadOnlySpan<byte> payload)
    {
        if (
            payload.Length < PayloadsConstants.MinClipboardPayloadSize
            || payload.Length > PayloadsConstants.MaxClipboardPayloadSize
        )
        {
            return null;
        }

        var deviceId = new Guid(payload[..PayloadsConstants.DeviceIdSize], bigEndian: true);

        string text;
        try
        {
            text = Utf8.GetString(payload[PayloadsConstants.DeviceIdSize..]);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }

        return (deviceId, text);
    }

    public static bool IsValid(MessageType type, ReadOnlySpan<byte> payload) =>
        type switch
        {
            MessageType.Ack => DecodeAck(payload) is not null,
            MessageType.Heartbeat => DecodeHeartbeat(payload) is not null,
            MessageType.Discover or MessageType.PeerInfo => DecodePeerInfo(payload) is not null,
            MessageType.FileOffer => DecodeFileOffer(payload) is not null,
            MessageType.FileAnswer => DecodeFileAnswer(payload) is not null,
            MessageType.Chunk => DecodeChunk(payload) is not null,
            MessageType.FileDone => DecodeFileDone(payload) is not null,
            MessageType.FileResult => DecodeFileResult(payload) is not null,
            MessageType.Clipboard => DecodeClipboard(payload) is not null,
            _ => false,
        };

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

    private static void WriteFileIds(byte[] buffer, Guid deviceId, Guid fileId)
    {
        deviceId.TryWriteBytes(
            buffer.AsSpan(0, PayloadsConstants.DeviceIdSize),
            bigEndian: true,
            out _
        );
        fileId.TryWriteBytes(
            buffer.AsSpan(PayloadsConstants.FileIdOffset, PayloadsConstants.FileIdSize),
            bigEndian: true,
            out _
        );
    }
}
