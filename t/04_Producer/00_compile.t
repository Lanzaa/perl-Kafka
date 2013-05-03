#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Tests load by Kafka::Producer

use lib 'lib';

use Test::More tests => 9;

# PRECONDITIONS ----------------------------------------------------------------

use Kafka::Mock;
use Kafka::IO;

# -- verification of the IO objects creation
my $server = Kafka::Mock->new(
    requests    => {},
    responses   => {},
    );
isa_ok( $server, 'Kafka::Mock' );

# INSTRUCTIONS -----------------------------------------------------------------

# -- verify load the module
BEGIN { use_ok 'Kafka::Producer' }

my $producer = Kafka::Producer->new(
    broker_list => "localhost:$server->port",
    );
unless ( $producer )
{
    fail "(".Kafka::Producer::last_errorcode().") ".Kafka::Producer::last_error();
    exit 1;
}
isa_ok( $producer, 'Kafka::Producer' );

# -- verify the availability of functions
can_ok( $producer, $_ ) for qw( new last_error last_errorcode send close );


# POSTCONDITIONS ---------------------------------------------------------------

# -- Closes and cleans up
$producer->close;
$server->close;
