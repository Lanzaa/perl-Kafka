#!/usr/bin/perl -w

use 5.010;
use strict;
use warnings;

# NAME: Test of the function Kafka::Protocol::offsets_request

use lib 'lib';

use Test::More tests => 32;

BEGIN {
    eval "use Test::Exception";
    plan skip_all => "because Test::Exception required for testing" if $@;
}

# PRECONDITIONS ----------------------------------------------------------------

# options for testing arguments: ( undef, 0, 0.5, 1, -1, -3, "", "0", "0.5", "1", \"scalar", [] )

# -- verify load the module
BEGIN { use_ok 'Kafka::Protocol', qw( offsets_request ) }

# -- declaration of variables to test
my $topic       = "test";
my $partition   = 0;
my $time        = -2;
my $max_number  = 100;

# control request
my $request     =                               # OFFSETS       Request
    # Request Header
     '00000036'                 # Request Length
    .'0002'                     # API Key
    .'0000'                     # API Version
    .'ffffffff'                 # Correlation ID
    .'000a'                     # Client ID Length
    .'7065726c2d6b61666b61'     # Client ID ("perl-kafka")
    # Offset Request
    .'ffffffff'     # Replica ID
    .'00000001' # Number of Topics
    .'0004' # Topic length
    .'74657374' # Topic ("test")
    .'00000001' # Number of Partitions
    .'00000000' # Partition
    .'fffffffffffffffe' # Time
    .'00000064' # Max Number of Offsets
    ;

# INSTRUCTIONS -----------------------------------------------------------------
# -- verify response to invalid arguments

# without args
throws_ok { offsets_request() }                                             qr/^Mismatch argument/, 'expecting to die: Mismatch argument';

# topic: to see if a value is a normal non-false string of non-zero length
foreach my $topic ( ( undef, 0, "", "0", \"scalar", [] ) )
{
    throws_ok { offsets_request( $topic, $partition, $time, $max_number ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# partition: to see if a value is a non-negative integer (of any length). That is, a positive integer, or zero
foreach my $partition ( ( undef, 0.5, -1, "", "0.5", \"scalar", [] ) )
{
    throws_ok { offsets_request( $topic, $partition, $time, $max_number ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# time (truncated to an integer):
#   to see if a value is a number. That is, it is defined and perl thinks it's a number
#   or "Math::BigInt" object
foreach my $time ( ( undef, -3, "", \"scalar", [] ) )
{
    throws_ok { offsets_request( $topic, $partition, $time, $max_number ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# max_number: to see if a value is a positive integer (of any length)
foreach my $max_number ( ( undef, 0, 0.5, -1, "", "0", "0.5", \"scalar", [] ) )
{
    throws_ok { offsets_request( $topic, $partition, $time, $max_number ) } qr/^Mismatch argument/, 'expecting to die: Mismatch argument';
}

# -- verify request
is unpack( "H*", offsets_request( $topic, $partition, $time, $max_number ) ), $request, "correct request";

# request with a bigint time
{
    use bigint;
    my $time = -2;
    isa_ok( $time, 'Math::BigInt');
    is unpack( "H*", offsets_request( $topic, $partition, $time, $max_number ) ), $request, "correct request";
}

# POSTCONDITIONS ---------------------------------------------------------------
