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

    $self->_init(); # initialize connections
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
    my @brokers = split(",", $self->{broker_list});
    foreach my $broker (@brokers) {
        # TODO: connect to broker
        my ($host, $port) = split(":", $broker);
        my $io = Kafka::IO->new(
            host => $host,
            port => $port,
            timeout => $self->{timeout},
        );
        if (defined($io)) {
            $self->{connections}->{$broker} = $io;
        } else {
            # TODO warn connection to broker failed
        }
    }
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

    ( # TODO argument checking
        1
    ) or return $self->_error(ERROR_MISMATCH_ARGUMENT);

}


1;
