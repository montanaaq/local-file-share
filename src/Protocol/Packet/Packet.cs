using System;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace Protocol;

public sealed record Packet(MessageType Type, uint Seq, byte[] Payload)
{
    public const int HeaderSize = 12; // Version, type, sequence, length and CRC: 1 + 1 + 4 + 2 + 4 bytes.
    public const int MaxPayloadSize =
        PayloadsConstants.ChunkDataOffset + ProtocolConstants.ChunkSize;

    public byte[] Serialize()
    {
        if (!Enum.IsDefined(Type))
        {
            throw new ArgumentException("Unknown message type", nameof(Type));
        }

        if (Payload.Length > MaxPayloadSize)
        {
            throw new ArgumentException(
                $"Payload size exceeds the maximum allowed size: {Payload.Length} > {MaxPayloadSize}",
                nameof(Payload)
            );
        }

        byte[] buffer = new byte[HeaderSize + Payload.Length];
        buffer[0] = 1;
        buffer[1] = (byte)Type;
        BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(PacketOffsets.SeqOffset), Seq);
        BinaryPrimitives.WriteUInt16BigEndian(
            buffer.AsSpan(PacketOffsets.LengthOffset),
            (ushort)Payload.Length
        );
        Payload.CopyTo(buffer, HeaderSize);
        BinaryPrimitives.WriteUInt32BigEndian(buffer.AsSpan(PacketOffsets.CrcOffset), Crc(buffer));
        return buffer;
    }

    public static Packet? Parse(ReadOnlySpan<byte> data)
    {
        if (
            data.Length < HeaderSize
            || data.Length > HeaderSize + MaxPayloadSize
            || data[0] != 1
            || !Enum.IsDefined((MessageType)data[1])
        )
        {
            return null;
        }

        if (
            data.Length
            != HeaderSize + BinaryPrimitives.ReadUInt16BigEndian(data[PacketOffsets.LengthOffset..])
        )
        {
            return null;
        }

        if (BinaryPrimitives.ReadUInt32BigEndian(data[PacketOffsets.CrcOffset..]) != Crc(data))
        {
            return null;
        }

        return new Packet(
            (MessageType)data[1],
            BinaryPrimitives.ReadUInt32BigEndian(data[PacketOffsets.SeqOffset..]),
            data[HeaderSize..].ToArray()
        );
    }

    private static uint Crc(ReadOnlySpan<byte> data)
    {
        byte[] copy = data.ToArray();
        copy.AsSpan(PacketOffsets.CrcOffset, sizeof(uint)).Clear();
        return Crc32.HashToUInt32(copy);
    }
}
