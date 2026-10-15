using System;

namespace Protocol;

internal readonly record struct TransferKey(Guid PeerDeviceId, Guid FileId);
