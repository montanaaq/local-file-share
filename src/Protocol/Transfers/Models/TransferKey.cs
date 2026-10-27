using System;

namespace Protocol;

public readonly record struct TransferKey(Guid PeerDeviceId, Guid FileId);
