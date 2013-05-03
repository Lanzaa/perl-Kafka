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
    metadata_request_ng
    request_header_encode
    );
use Kafka::IO;

our $DEBUG = 1;

BEGIN { if($DEBUG) { use Data::Dumper; } }

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
    my @topics = @_;
    my @brokers = split(",", $self->{broker_list});
    # There are several stages
    # 1. Bootstrap - use the list of brokers to find all the other brokers. we
    #       will stop at the first successfull connection
    # 2. Metadata Grab - use the established connection to grab metadata for
    #       the broker set. cache the topic metadata
    # 3. Connect - connect to all of the brokers specified in the metadata.
    #       maintain these connections in a hash { brokerId => Kafka::IO }
    my $io = undef;
    foreach my $broker (@brokers) {
        my ($host, $port) = split(":", $broker);
        $io = Kafka::IO->new(
            host => $host,
            port => $port,
            timeout => $self->{timeout}, # XXX: do we need a timeout here?
        );
        if (defined($io)) {
            last; # We have a connection, get out of here
        } else {
            # TODO warn connection to boostrap broker failed
        }
    }
    # should have a valid IO connection
    if (!defined($io)) {
        croak("Unable to connect to a valid Kafka broker.");
    }
    my $metadata = $io->request_metadata(@topics);

    my $ret = $self->_refreshMetadata($io, @topics);

    print STDERR "metadata: ".Dumper($metadata);
    print STDERR "ret: ".Dumper($ret);



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
    confess("sendRequest is not yet implemented");

    my $io = $self->{connections}->{$brokerId};
    if (defined($io)) {
        return $self->_sendIO($io, $apiKey, $dataRef);
    }
    confess("Unable to send request, unknown brokerId.");
    return -1;
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
        $self->{clientId} || "CLIENTIDFORKAFKAPERL", # XXX create a client id
        $correlationId,
    ).$$dataRef;

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
    my $message = $io->receive(unpack("N", $$packedSize));
    unless($message and defined($$message)) {
        confess("Something died in receiveIO");
        return -1; # XXX some error code
    }
    my $data = {
        correlationId => unpack("N", $$message),
        data => $$message,
        position => 4, # just unpacked 4 bytes from the message
    };
    return $data;
}



##
# Used to refresh metadata
#
# Expects:
#   * Kafka::IO
# Returns:
# TODO messages!
##
# TODO: impl, test
sub _refreshMetadata {
    my $self = shift;
    my ($io, @topics) = @_;
    my $data = metadata_request_ng(\@topics);
    my $correlationId = 9; # XXX get a real id
    my $error = $self->_sendIO($io, APIKEY_METADATA, $correlationId, $data);

    my $received = $self->_receiveIO($io);
    print STDERR "received some data: ".Dumper(\$received);

}


1;
