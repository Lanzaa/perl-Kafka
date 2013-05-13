package Kafka::BrokerChannel;

use 5.010;
use strict;
use warnings;

# Basic functionalities to connect to a broker group

use Carp;
use Params::Util qw( _STRING );

use Kafka qw(
    DEFAULT_TIMEOUT
    );
use Kafka::Protocol qw(
    APIKEY_METADATA
    CLIENT_ID
    metadata_request_ng
    metadata_response_ng
    request_header_encode
    );
use Kafka::IO;

use constant DEBUG => 0;
use constant DEBUG_IO => 0; # XXX Used for dumping the bytes that are being sent over the wire.

BEGIN { if(DEBUG) { use Data::Dumper; } }

##
#
# should expect a broker list. a comma separated list of server:port
# brokerlist = server01:port,server02:port,server03:port
#
##
# TODO: pod, impl
# * allow passing of a list of topics
sub new {
    my ($class, %opt) = @_;

    my $self = {
        timeout => DEFAULT_TIMEOUT, # timeout for connections
        broker_list => undef, # user supplied list of brokers
        connections => {}, # internal connection hash
        # metadata => ?
    };
    bless($self, $class);

    # TODO: impl
    # * allow set timeout

    unless (defined( _STRING( $opt{broker_list}))) {
        croak("The broker_list must be supplied as a string.");
    }
    $self->{broker_list} = $opt{broker_list};

    # Bootstrap and initialize all connections
    $self->_init([]); # TODO allow passing of limited topics
    # TODO: implement
    #die("[BUG] brokerchannel is not implemented");
    return $self;
}

##
# Used to initialize connections to the brokers.
##
# TODO: warnings
sub _init {
    my $self = shift;
    my $topics = shift;
    my @brokers = split(",", $self->{broker_list});
    # There are several stages
    # 1. Bootstrap - use the list of brokers to find all the other brokers. we
    #       will stop at the first successfull connection
    # 2. Metadata Grab - use the established connection to grab metadata for
    #       the broker set. cache the topic metadata
    # 3. Connect - connect to all of the brokers specified in the metadata.
    #       maintain these connections in a hash { brokerId => Kafka::IO }
    my $io = undef;

    # 1. Bootstrap
    foreach my $broker (@brokers) {
        my ($host, $port) = split(":", $broker);
        $io = Kafka::IO->new(
            host => $host,
            port => $port,
            timeout => $self->{timeout}, # XXX proper timeout
            RaiseError => 1,
        );
        if (defined($io)) {
            last; # We have a connection, get out of here
        } else {
            # TODO warn connection to boostrap broker failed
            warn("Unable to bootstrap with broker: '$host:$port'");
        }
    }
    # should have a valid IO connection
    if (!defined($io)) {
        croak("Unable to connect to a valid Kafka broker.");
    }

    # 2. Grab metadata
    $self->_refreshMetadata($io, @$topics);

    # 3. Connect
    my $broker_metadata = $self->{metadata}->{brokers};
    foreach my $brokerId (keys $broker_metadata) {
        my ($host, $port) = @{$broker_metadata->{$brokerId}};
        my $broker_io = Kafka::IO->new(
            host => $host,
            port => $port,
            timeout => $self->{timeout},
            RaiseError => 1,
        );
        if (defined($io)) {
            $self->{connections}->{$brokerId} = $broker_io;
        } else {
            warn("Unable to connect to remote broker '$host:$port'\n");
        }
    }

    return;
}

##
# Used to close theconnection to all brokers
##
# TODO: warnings, errors
sub close {
    my $self = shift;
    foreach my $io (values $self->{connections}) {
        if (defined($io)) {
            $io->close();
        }
    }
}

##
# Used to send a request to a broker.
#
# Expects:
#   * brokerId - Id of the broker to send the request to
#   * apiKey - the apikey of the request type
#   * dataRef - a reference to the packed request
# Returns:
#   TODO messages!
##
# TODO: impl, pod
sub sendSyncRequest {
    my ($self, $brokerId, $apiKey, $dataRef) = @_;

    my $io = $self->{connections}->{$brokerId};
    if (!defined($io)) {
        confess("Unable to send request, unknown brokerId.");
    }
    my $correlationId = $self->_getCorrId();
    if (DEBUG) {
        print STDERR "Writing to broker with id '$brokerId'\n";
    }
    my $send_error = $self->_sendIO($io, $apiKey, $correlationId, $dataRef);
    my $response = $self->_receiveIO($io);
    if (!defined($response) || $response->{correlationId} != $correlationId) {
        # Note: Since we are only sending one request at a time this should never happen.
        confess("[BUG] sendSyncRequest did not receive the proper correlation id.");
    }
    return $response;
}

##
# Used to send a request over a specific IO connection.
#
# Expects:
#   * Kafka::IO
#   * a ref to the packed request
##
# TODO: impl, notes, test
sub _sendIO {
    my $self = shift;
    my ($io, $apiKey, $correlationId, $dataRef) = @_;

    # TODO pack headers
    my $encoded = request_header_encode(
        bytes::length( $$dataRef ),
        $apiKey,
        $self->{clientId} || CLIENT_ID,
        $correlationId,
    ).$$dataRef;

    if (DEBUG_IO) {
        print STDERR "Sending data: '".unpack("H*", $encoded)."'\n";
    }
    my $error = $io->send($encoded);
    return $error;
}

##
# Used to receive a response over a specific IO connection.
#
# Expects:
#   * Kafka::IO
# Returns:
#   the packed data corresponding to the request (see code for format)
##
sub _receiveIO {
    my $self = shift;
    my $io = shift;
    my $packedSize = $io->receive(4); # Get the size
    if (!defined($packedSize)) {
        confess("[BUG] something died in receiveIO");
    }
    my $message = $io->receive(unpack("N", $$packedSize));
    unless($message and defined($$message)) {
        confess("Something died in receiveIO");
    }
    if (DEBUG_IO) {
        print STDERR "Incoming data: '".unpack("H*", $$packedSize).unpack("H*", $$message)."'\n";
    }
    my $data = {
        correlationId => unpack("N", $$message),
        data => $$message,
        position => 4, # just unpacked 4 bytes from the message
    };
    return $data;
}

##
# Used to just get a list of topics that this object knows about
#
# Expects:
# Returns:
#   * An array of strings representing topics
##
# TODO: docs
sub topic_list {
    my $self = shift;
    my @topics = keys $self->{metadata}->{topics};
    return @topics;
}

##
# Used to refresh metadata
#
# Expects:
#   * Kafka::IO
# Returns:
#   Nothing
##
# TODO: test
sub _refreshMetadata {
    my $self = shift;
    my ($io, @topics) = @_;
    my $data = metadata_request_ng(\@topics);
    my $correlationId = $self->_getCorrId();
    my $error = $self->_sendIO($io, APIKEY_METADATA, $correlationId, $data);
    my $received = $self->_receiveIO($io);
    if ($received->{correlationId} != $correlationId) {
        confess("Something went wrong when requesting metadata.");
    }
    my $metadata = metadata_response_ng($received);
    $self->{metadata} = $metadata;
    $self->{metadata}->{refreshed} = time();
}

##
# Used to generate a correlation Id. This should be more useful in the future
# if there are ever more than one request in flight to a server.
##
sub _getCorrId {
    my $self = shift;
    return int(rand(20000000));
}

##
# Used to list known metadata about a specific topic.
#
# Expects:
#   * a topic name
# Returns:
#   * a hash: {
#       partitionId => brokerId,
#       partitionId => brokerId,
#   }
##
# TODO: impl, test, doc
sub getTopicMetadata {
    my ($self, $topic) = @_;
    my $ret = $self->{metadata}->{topics}->{$topic};
    if (!defined($ret)) {
        confess("[BUG] Need to try and reload metadata in getTopicMetadata.");
    }
    return $ret->{partitions};
}


1;
