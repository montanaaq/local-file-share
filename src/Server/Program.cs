using System;
using Protocol;

var bytes = new Packet(MessageType.Clipboard, 42, [1, 2, 3]).Serialize();
Console.WriteLine(Packet.Parse(bytes)?.Seq == 42); // True
bytes[^1] ^= 0xFF;
Console.WriteLine(Packet.Parse(bytes) is null); // True
