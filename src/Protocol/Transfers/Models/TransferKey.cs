using System;

namespace Protocol.Transfers.Models;

public readonly record struct TransferKey(Guid PeerDeviceId, Guid FileId);
