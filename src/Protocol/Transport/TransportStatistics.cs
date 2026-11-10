namespace Protocol.Transport;

public sealed record TransportStatistics(
    long SentPackets,
    long ReceivedPackets,
    long AcknowledgmentsSent,
    long AcknowledgmentsReceived,
    long Retries,
    long DroppedOutgoingPackets,
    long DroppedIncomingPackets,
    long InvalidPackets,
    long QueueOverflows,
    long Errors
);
