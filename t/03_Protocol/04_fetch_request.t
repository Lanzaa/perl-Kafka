#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::fetch_request

use lib 'lib';

use Test::More tests => 34;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# options for testing arguments: ( undef, 0, 0.5, 1, -1, "", "0", "0.5", "1", \"scalar", [] )

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol', qw( fetch_request ) }

# -- declaration of variables to test
my $topic       = "test";
my $partition   = 0;
my $offset      = 0;
my $max_size    = 1024 * 1024;

# control request
my $request     =                               # FETCH         Request
    # Request Header
     '0000003e'                 # Request length
    .'0001'                     # API Key 
    .'0000'                     # API Version
    .'ffffffff'                 # Correlation ID
    .'000a'                     # Client ID Length
    .'7065726c2d6b61666b61'     # Client ID ("perl-kafka")
    # Fetch Request
    .'ffffffff'                 # Replica ID
    .'00000002'                 # Max Wait Time
    .'00000001'                 # Min Bytes
    # Topic Request
    .'00000001' # Number of topics
    .'0004' # Topic length
    .'74657374' # Topic ("test")
    .'00000001' # Number of partitions
    .'00000000' # Patition
    .'0000000000000000' # Fetch Offset
    .'00100000' # Max Bytes
    ;

# INSTRUCTIONS -----------------------------------------------------------------
# -- verify response to invalid arguments

# without args
throws_ok { fetch_request() }                                               qr/^Mismatch argument/, 'expecting to die: Mismatch argument';

# topic: to see if a value is a normal non-false string of non-zero length
foreach my $topic ( ( undef, 0, "", "0", \"scalar", [] ) )
{
    throws_ok { fetch_request( $topic, $partition, $offset, $max_size ) }   qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# partition: to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
foreach my $partition ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    throws_ok { fetch_request( $topic, $partition, $offset, $max_size ) }   qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# offset:
#   to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
#   or "Math::BigInt" object
foreach my $offset ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    throws_ok { fetch_request( $topic, $partition, $offset, $max_size ) }   qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# max_size: to see if a value is a positive integer (of any length)
foreach my $max_size ( ( undef, 0, 0.5, -1, "", "0", "0.5", \"scalar", [] ) )
{
    throws_ok { fetch_request( $topic, $partition, $offset, $max_size ) }   qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# -- verify request
is unpack( "H*", fetch_request( $topic, $partition, $offset, $max_size ) ), $request, "correct request";

# request with a bigint offset
{
    use bigint;
    my $offset = 0;
    isa_ok( $offset, 'Math::BigInt');
    is unpack( "H*", fetch_request( $topic, $partition, $offset, $max_size ) ), $request, "correct request";
}

# POSTCONDITIONS ---------------------------------------------------------------
